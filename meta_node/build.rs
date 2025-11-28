fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/raft_service.proto")?;
    tonic_build::compile_protos("proto/mngmt_service.proto")?;
    tonic_build::compile_protos("proto/stream_service.proto")?;
    tonic_build::compile_protos("proto/dnode_service.proto")?;
    Ok(())
}
