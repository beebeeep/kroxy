pub mod config;
pub(crate) mod consumer;
pub(crate) mod producer;
pub mod server;
pub mod grpc {
    tonic::include_proto!("kroxy");
}
