use std::convert::TryFrom;
use bytes::Buf;

#[derive(Clone, Copy)]
#[repr(u32)]
pub enum ChecksumKind {
    None,
    CRC32,
}

impl ChecksumKind {
    pub const fn size(&self) -> usize {
        match self {
            Self::None => 0,
            Self::CRC32 => 4,
        }
    }
    pub fn hash(&self, data: &[u8]) -> ChecksumValue {
        match self {
            Self::None => ChecksumValue::None,
            Self::CRC32 => {
                let val = crc32fast::hash(data);
                ChecksumValue::CRC32(val.to_le_bytes())
            }
        }
    } 
}

impl TryFrom<u32> for ChecksumKind {
    type Error = ();
    fn try_from(val: u32) -> Result<Self, Self::Error> {
        match val {
            val if val == ChecksumKind::None as u32 => {
                Ok(ChecksumKind::None)
            }
            val if val == ChecksumKind::CRC32 as u32 => {
                Ok(ChecksumKind::CRC32)
            }
            _ => {
                Err(())
            }
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub enum ChecksumValue {
    None,
    CRC32([u8; 4]),
}

impl ChecksumValue {
    pub fn read<B: Buf>(buf: &mut B, kind: ChecksumKind) -> Self {
        match kind {
            ChecksumKind::None => ChecksumValue::None,
            ChecksumKind::CRC32 => {
                let val = buf.get_u32_le();
                ChecksumValue::CRC32(val.to_le_bytes())
            }
        }
    }
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::None => &[],
            Self::CRC32(val) => val
        }
    }
}