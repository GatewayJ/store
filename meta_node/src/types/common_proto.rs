pub use common_proto::*;

#[repr(u8)]
enum PrefixSpace1 {
    DataNode = b'N',
    Volume = b'V',
    VolumeExtent = b'E',
}

pub trait MetaStoreKey<const KLEN: usize> : Sized {
    const KEY_PREFIX: u8;
    const KEY_LEN: usize = KLEN;

    fn store_key(&self) -> [u8; KLEN];
    fn parse(_buf: &[u8]) -> Option<Self> {
        None
    }
}

impl MetaStoreKey<5> for DataNodeId {
    const KEY_PREFIX: u8 = PrefixSpace1::DataNode as u8;

    fn store_key(&self) -> [u8; 5] {
        let mut key: [u8; 5] = Default::default();
        key[0] = Self::KEY_PREFIX;
        key[1..].clone_from_slice(&self.id.to_be_bytes());
        key   
    }

    fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() == Self::KEY_LEN || buf[0] == Self::KEY_PREFIX {
            let mut id = u32::from_be_bytes(buf[1..].try_into().unwrap());
            id &= 0xffff0000;
            Some(Self { id })
        } else {
            None
        }
    }
}

pub trait VolumeIdStoreKey : Sized {
    const KEY_PREFIX: u8 = PrefixSpace1::Volume as u8;
    const KEY_LEN: usize = 6;
    const EXTENT_KEY_PREFIX: u8 = PrefixSpace1::VolumeExtent as u8;

    fn store_key(&self, subfix: u8) -> [u8; 6];
    fn parse(buf: &[u8]) -> Option<(Self, u8)>;
    fn extent_store_key(&self, ext: &ExtentId) -> [u8; 17];
}

impl VolumeIdStoreKey for VolumeId {
    const KEY_PREFIX: u8 = PrefixSpace1::Volume as u8;

    fn store_key(&self, subfix: u8) -> [u8; 6] {
        let mut key: [u8; 6] = Default::default();
        key[0] = Self::KEY_PREFIX;
        key[1..5].clone_from_slice(&self.id.to_be_bytes());
        key[5] = subfix;
        key
    }

    fn parse(buf: &[u8]) -> Option<(Self, u8)> {
        if buf.len() == Self::KEY_LEN && buf[0] == Self::KEY_PREFIX {
            let id = u32::from_be_bytes(buf[1..5].try_into().unwrap());
            Some((Self { id }, buf[5]))
        } else {
            None
        }
    }

    fn extent_store_key(&self, ext: &ExtentId) -> [u8; 17] {
        let mut key: [u8; 17] = Default::default();
        key[0] = Self::EXTENT_KEY_PREFIX; 
        key[1..5].copy_from_slice(&self.id.to_be_bytes());
        key[5..9].copy_from_slice(&ext.stream_id.to_be_bytes());
        key[9..].copy_from_slice(&ext.log_seq.to_be_bytes());
        key
    }
}

#[repr(u8)]
enum PrefixSpace2 {
    LogStream = b'S',
    Extent = b'E',
    Client = b'C',
    ClientStream = b'O',
}

impl MetaStoreKey<5> for LogStreamId {
    const KEY_PREFIX: u8 = PrefixSpace2::LogStream as u8;

    fn store_key(&self) -> [u8; 5] {
        let mut key: [u8; 5] = Default::default();
        key[0] = Self::KEY_PREFIX;
        key[1..5].clone_from_slice(&self.id.to_be_bytes());
        key
    }

    fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() == 5 && buf[0] == Self::KEY_PREFIX {
            let id = u32::from_be_bytes(buf[1..5].try_into().unwrap());
            Some(Self { id })
        } else {
            None
        }
    }
}

impl MetaStoreKey<13> for ExtentId {
    const KEY_PREFIX: u8 = PrefixSpace2::Extent as u8;

    fn store_key(&self) -> [u8; 13] {
        let mut key: [u8; 13] = Default::default();
        key[0] = Self::KEY_PREFIX;
        key[1..5].clone_from_slice(&self.stream_id.to_be_bytes());
        key[5..].clone_from_slice(&self.log_seq.to_be_bytes());
        key
    }
}

impl MetaStoreKey<9> for ClientId {
    const KEY_PREFIX: u8 = PrefixSpace2::Client as u8;

    fn store_key(&self) -> [u8; 9] {
        let mut key: [u8; 9] = Default::default();
        key[0] = Self::KEY_PREFIX;
        key[1..9].clone_from_slice(&self.id.to_be_bytes());
        key
    }

    fn parse(buf: &[u8]) -> Option<Self> {
        if buf.len() == 9 && buf[0] == Self::KEY_PREFIX {
            let id = u64::from_be_bytes(buf[1..9].try_into().unwrap());
            Some(Self::new(id))
        } else {
            None
        }
    }
}

pub trait ClientIdStoreKey : MetaStoreKey<9> {
    const STREAM_KEY_PREFIX: u8 = PrefixSpace2::ClientStream as u8;
    fn last_id_key() -> [u8; 9];
    fn stream_store_key(&self, stream_id: &LogStreamId) -> [u8; 13];
    fn parse_stream(buf: &[u8]) -> Option<(Self, LogStreamId)>;
}

impl ClientIdStoreKey for ClientId {
    fn last_id_key() -> [u8; 9] {
        let mut key: [u8; 9] = Default::default();
        key[0] = Self::KEY_PREFIX;
        key[1..9].clone_from_slice(&u64::MAX.to_be_bytes());
        key
    }

    fn stream_store_key(&self, stream_id: &LogStreamId) -> [u8; 13] {
        let mut key: [u8; 13] = Default::default();
        key[0] = Self::STREAM_KEY_PREFIX; 
        key[1..9].copy_from_slice(&self.id.to_be_bytes());
        key[9..13].copy_from_slice(&stream_id.id.to_be_bytes());
        key
    }

    fn parse_stream(buf: &[u8]) -> Option<(Self, LogStreamId)> {
        if buf.len() == 13 && buf[0] == Self::STREAM_KEY_PREFIX {
            let id = u64::from_be_bytes(buf[1..9].try_into().unwrap());
            let stream_id = u32::from_be_bytes(buf[9..13].try_into().unwrap());
            Some((Self::new(id), LogStreamId::new(stream_id)))
        } else {
            None
        }
    }
}