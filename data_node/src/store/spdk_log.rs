use std::mem::{transmute, MaybeUninit};
use crate::types::common_proto::ExtentVersion;
use super::checksum::{ChecksumKind, ChecksumValue};
use bytes::{BufMut, Buf};
use nix::errno::Errno;
use tracing::error;

const LOG_HEADER_ALIGN: usize = 8;

fn roundup_aligned(size: usize) -> usize {
    size.next_multiple_of(LOG_HEADER_ALIGN)
}

fn data_csums_size(
    data_len: u32, io_unit_size: usize, csum_kind: ChecksumKind
) -> usize {
    data_len as usize / io_unit_size * csum_kind.size()
}

pub struct LogHeaderWriter<'a> {
    buffer: &'a mut [MaybeUninit<u8>],
    data_len: u32,
    user_header_len: u16,
    header_len: u16,

    data_csums_len: usize,
    io_unit_size: usize,
    csum_kind: ChecksumKind,
}

impl<'a> LogHeaderWriter<'a> {
    const METADATA_SPACE: usize = 128;

    pub fn new(
        buffer: &'a mut [MaybeUninit<u8>], data_len: usize,
        io_unit_size: usize, csum_kind: ChecksumKind, 
    ) -> Self {
        let data_csums_len = data_csums_size(
            data_len as u32, io_unit_size, csum_kind
        );
        Self {
            buffer,
            data_len: data_len as u32,
            user_header_len: 0,
            header_len: io_unit_size as u16,
            data_csums_len,
            io_unit_size,
            csum_kind,
        }
    }

    pub fn user_header_max_size(
        data_len: usize, io_unit_size: usize, csum_kind: ChecksumKind
    ) -> Result<usize, Errno> {
        let data_csums_space = roundup_aligned(
            data_csums_size(data_len as u32, io_unit_size, csum_kind)
        );
        if io_unit_size >= Self::METADATA_SPACE + data_csums_space {
            Ok(io_unit_size - Self::METADATA_SPACE - data_csums_space)
        } else {
            Err(Errno::ENOSPC)
        }
    }

    pub fn put_user_header(&mut self, user_hdr: &[u8]) -> Result<(), Errno> {
        let mut buf = self.alloc_user_header(user_hdr.len())?;
        buf.put_slice(user_hdr);
        Ok(())
    }

    pub fn alloc_user_header(&mut self, user_hdr_size: usize) -> Result<&mut [MaybeUninit<u8>], Errno> {
        if self.header_len == 0 {
            return Err(Errno::EINVAL);
        }
        let data_csums_space = roundup_aligned(self.data_csums_len);
        let user_hdr_space = roundup_aligned(user_hdr_size);
        if Self::METADATA_SPACE + data_csums_space + user_hdr_space > self.header_len as usize {
            return Err(Errno::ENOSPC);
        }
        let user_hdr_off = self.header_len as usize - data_csums_space - user_hdr_space;
        let (buf, mut padding) = self.buffer
            .split_at_mut(user_hdr_off)
            .1[..user_hdr_space]
            .split_at_mut(user_hdr_size);
        if !padding.is_empty() {
            padding.put_bytes(0, padding.len())
        }
        self.user_header_len = user_hdr_size as u16;
        Ok(buf)
    }

    pub fn calc_data_csums(&mut self, data: &[u8]) -> Result<(), Errno> {
        if self.header_len == 0 {
            return Err(Errno::EINVAL);
        }
        if data.len() != self.data_len as usize {
            return Err(Errno::EINVAL);
        }
        let data_csums_space = roundup_aligned(self.data_csums_len);
        if Self::METADATA_SPACE + data_csums_space >= self.header_len as _ {
            return Err(Errno::ENOSPC);
        }
        let (_hdr_buf, mut buf) = self.buffer.split_at_mut(
            self.header_len as usize - data_csums_space
        );

        for i in 0..data.len() / self.io_unit_size {
            let j = i + 1;
            let data_csum = self.csum_kind.hash(
                &data[(self.io_unit_size * i)..(self.io_unit_size * j)]
            );

            buf.put_slice(data_csum.as_slice());
        }
        if data_csums_space != self.data_csums_len {
            buf.put_bytes(0, data_csums_space - self.data_csums_len);
        }
        Ok(())
    }

    pub fn put_metadata(
        &mut self, sentinel: u64, stream_id: u32,
        version: ExtentVersion, data_offset: u32
    ) {
        let hdr_csum_space = roundup_aligned(self.csum_kind.size());
        let data_csums_space = roundup_aligned(self.data_csums_len);
        let user_hdr_space = roundup_aligned(self.user_header_len as usize);
        let orig_buf_len = self.buffer.remaining_mut();

        let (_, mut buf) = self.buffer.split_at_mut(hdr_csum_space);
        buf.put_u64_le(sentinel);
        buf.put_u32_le(stream_id);
        buf.put_u32_le(version.epoch);
        buf.put_u64_le(version.log_seq);
        buf.put_u32_le(data_offset);
        buf.put_u32_le(self.data_len);
        buf.put_u16_le(self.user_header_len);

        let mut curr_off = orig_buf_len - buf.remaining_mut();
        let meta_space = roundup_aligned(curr_off + 2);
        assert!(meta_space <= Self::METADATA_SPACE);

        let padding_len = if self.header_len > 0 {
            buf.put_u16_le(self.header_len);
            self.header_len as usize - meta_space - data_csums_space - user_hdr_space
        } else {
            self.header_len = (meta_space + user_hdr_space + data_csums_space) as u16;
            buf.put_u16_le(self.header_len);
            curr_off += 2;
            meta_space - curr_off
        };
        if padding_len > 0 {
            buf.put_bytes(0, padding_len);
        }
    }

    pub fn finalize(self) -> &'a [u8] {
        let hdr_csum_len = self.csum_kind.size();
        let hdr_csum_space = roundup_aligned(hdr_csum_len);

        let (mut csum_buf, buf) = self.buffer.split_at_mut(hdr_csum_space);
        let hdr_csum = self.csum_kind.hash(
            unsafe { transmute(buf) }
        );
        csum_buf.put_slice(hdr_csum.as_slice());
        if hdr_csum_len < hdr_csum_space {
            csum_buf.put_bytes(0, hdr_csum_space - hdr_csum_len)
        }

        unsafe { transmute(&self.buffer[..self.header_len as usize]) }
    }
}

pub struct LogHeaderReader<'a> {
    buffer: &'a [u8],
    io_unit_size: usize,
    csum_kind: ChecksumKind,
    sentinel: u64,
    stream_id: u32,
    version: ExtentVersion,
}

impl<'a> LogHeaderReader<'a> {
    pub fn new(
        buffer: &'a [u8], io_unit_size: usize, csum_kind: ChecksumKind,
        sentinel: u64, stream_id: u32, version: ExtentVersion
    ) -> Self {
        Self { buffer, io_unit_size, csum_kind, sentinel, stream_id, version }
    }

    pub fn get(&mut self) -> Option<LogHeader<'a>> {
        let hdr_csum_space = roundup_aligned(self.csum_kind.size());
        if self.buffer.remaining() < hdr_csum_space + 32 {
            return None;
        }

        let mut buf = self.buffer;
        buf.advance(hdr_csum_space);
        let sentinel = buf.get_u64_le();
        let stream_id = buf.get_u32_le();
        let epoch = buf.get_u32_le();
        let log_seq = buf.get_u64_le();

        if sentinel != self.sentinel ||
            stream_id != self.stream_id ||
            epoch != self.version.epoch ||
            log_seq != self.version.log_seq {
            return None;
        }

        let data_offset = buf.get_u32_le();
        let data_len = buf.get_u32_le();
        let user_header_len = buf.get_u16_le();
        let header_len = buf.get_u16_le();

        if self.buffer.remaining() < header_len as usize {
            return None;
        }

        let mut log_header = LogHeader {
            _sentinel: sentinel,
            _version: ExtentVersion { epoch, log_seq },
            _stream_id: stream_id,
            data_offset,
            data_len,
            _user_header_len: user_header_len,
            header_len,
            buffer: &self.buffer[..header_len as usize],
            data_csums: None,
            user_header: None,
        };

        let curr_off = self.buffer.remaining() - buf.remaining();

        let data_csums_len = data_csums_size(data_len, self.io_unit_size, self.csum_kind);
        let data_csums_space = roundup_aligned(data_csums_len);
        let user_hdr_space = roundup_aligned(user_header_len as usize);

        if header_len as usize >=
           roundup_aligned(curr_off) + data_csums_space + user_hdr_space {
            let user_hdr_off = header_len as usize - data_csums_space - user_hdr_space;
            buf = &self.buffer[user_hdr_off..header_len as usize];
            log_header.user_header.replace(&buf[..user_header_len as usize]);
            log_header.data_csums.replace(&buf[user_hdr_space..]);
        }

        self.buffer.advance(log_header.header_len as usize);
        self.version.log_seq += 1;

        Some(log_header)
    }
}

pub struct LogHeader<'a> {
    pub _sentinel: u64,
    pub _stream_id: u32,
    pub _version: ExtentVersion,
    pub data_offset: u32,
    pub data_len: u32,
    pub _user_header_len: u16,
    pub header_len: u16,

    buffer: &'a [u8],
    pub user_header: Option<&'a [u8]>,
    pub data_csums: Option<&'a [u8]>,
}

impl<'a> LogHeader<'a> {
    pub fn check_header_csum(&self, csum_kind: ChecksumKind) -> bool {
        let hdr_csum_len = csum_kind.size();

        let (csum_buf, buf) = self.buffer.split_at(roundup_aligned(hdr_csum_len));
        let hdr_csum = csum_kind.hash(buf);

        csum_buf[..hdr_csum_len] == *hdr_csum.as_slice()
    }

    pub fn check_data_csums(
        &self, csum_kind: ChecksumKind,
        data: &[u8], data_offset: u32, io_unit_size: usize,
    ) -> Result<(), u32> {
        if data.is_empty() {
            return Ok(());
        }

        let csum_len = csum_kind.size();
        let csum_off = (data_offset - self.data_offset) as usize * csum_len;

        let mut data_csums = match self.data_csums {
            None => {
                return Err(u32::MAX);
            }
            Some(csums) => &csums[csum_off..],
        };

        for i in 0..data.len() / io_unit_size {
            if csum_len > data_csums.len() {
                return Err(data_offset + i as u32);
            }
            let data_csum = csum_kind.hash(&data[(io_unit_size * i)..(io_unit_size * (i + 1))]);
            let expected_csum = ChecksumValue::read(&mut data_csums, csum_kind);
            if data_csum != expected_csum {
                error!("checksum error {:?} {:?}", data_csum, expected_csum);
            }
        }
        Ok(())
    }
}
