use tonic::transport::Channel;

pub mod proto {
    // Incorporate the generated code compiled by build.rs
    tonic::include_proto!("oryx");
}

use proto::oryx_service_client::OryxServiceClient;
use std::boxed::Box;
use std::convert::TryInto;
use std::error;
use tonic::transport;

/// A client for communicating with a oryx (distributed key-value) store node.
#[derive(Debug, Clone)]
pub struct OryxClient {
    client: OryxServiceClient<Channel>,
}

impl OryxClient {
    /// Open a connection to a oryx node.
    ///
    /// ## Arguments
    /// * `dst` - The endpoint destination, e.g. `"http://localhost:50051"`.
    ///
    /// ## Errors
    /// Returns a `tonic::transport::Error` if connection fails.
    pub async fn connect<D>(dst: D) -> Result<Self, transport::Error>
    where
        D: TryInto<transport::Endpoint>,
        D::Error: Into<Box<dyn error::Error + Send + Sync>>,
    {
        let client = OryxServiceClient::connect(dst).await?;
        Ok(Self { client })
    }

    /// Retrieve the value associated with `key`.
    ///
    /// ## Arguments
    /// * `key` - The string slice representing the key.
    ///
    /// ## Returns
    /// * `Ok(Some(Vec<u8>))` if the key exists.
    /// * `Ok(None)` if the key does not exist.
    ///
    /// # Errors
    /// Returns a `tonic::Status` if the gRPC invocation fails.
    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, tonic::Status> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(proto::GetRequest {
            key: key.to_string(),
        });
        let response = client.get(request).await?.into_inner();

        if response.exists {
            Ok(Some(response.value))
        } else {
            Ok(None)
        }
    }

    /// Store a binary `value` under `key`.
    ///
    /// ## Arguments
    /// * `key` - The string slice representing the key.
    /// * `value` - The byte vector representing the value.
    ///
    /// ## Errors
    /// Returns a `tonic::Status` if the gRPC invocation fails.
    pub async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(proto::SetRequest {
            key: key.to_string(),
            value,
            timestamp: 0,
            node_id: String::new(),
        });
        client.set(request).await?;
        Ok(())
    }

    /// Remove a `key` and its associated value from the store.
    /// Resolves successfully even if the key did not exist.
    ///
    /// ## Arguments
    /// * `key` - The string slice representing the key to delete.
    ///
    /// ## Errors
    /// Returns a `tonic::Status` if the gRPC invocation fails.
    pub async fn delete(&self, key: &str) -> Result<(), tonic::Status> {
        let mut client = self.client.clone();
        let request = tonic::Request::new(proto::DeleteRequest {
            key: key.to_string(),
            timestamp: 0,
            node_id: String::new(),
        });
        client.delete(request).await?;
        Ok(())
    }
}
