//! Module to interact with a cosmos based blockchain.
//!
//! This module provide a client that can be used to perform requests to a cosmos based blockchain.

use crate::error::CosmosError;
use crate::json::{NodeInfo, NodeInfoResponse};
use cosmos_sdk_proto::cosmos::{
    auth::v1beta1::{query_client::QueryClient, BaseAccount, QueryAccountRequest},
    base::abci::v1beta1::TxResponse,
    tx::v1beta1::{service_client::ServiceClient, BroadcastMode, BroadcastTxRequest, Tx, TxRaw},
};
use reqwest::{get, StatusCode};
use tonic::Request;

/// Client to communicate with a full node.
#[derive(Clone)]
pub struct CosmosClient {
    grpc_addr: String,
    lcd_addr: String,
}

impl CosmosClient {
    /// Create a new CosmosClient instance that communicates with the full node.  
    /// The client will communicate using `lcd_address` for the legacy LCD requests
    /// and `grpc_addr` for the new gRPC request.
    ///
    ///# Examples
    ///
    /// ```
    ///  use crw_client::client::CosmosClient;
    ///
    ///  let client = CosmosClient::new("http://localhost:1317", "http://localhost:9090");
    /// ```
    pub fn new(lcd_addr: &str, grpc_addr: &str) -> CosmosClient {
        CosmosClient {
            grpc_addr: grpc_addr.to_string(),
            lcd_addr: lcd_addr.to_string(),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn grpc_client(&self) -> Result<tonic::transport::Channel, CosmosError> {
        let grpc_uri = self
            .grpc_addr
            .parse::<tonic::codegen::http::Uri>()
            .map_err(|err| CosmosError::Grpc(err.to_string()))?;
        let grpc_channel = tonic::transport::Channel::builder(grpc_uri);

        Ok(grpc_channel
            .connect()
            .await
            .map_err(|err| CosmosError::Grpc(err.to_string()))?)
    }

    #[cfg(target_arch = "wasm32")]
    async fn grpc_client(&self) -> Result<grpc_web_client::Client, CosmosError> {
        Ok(grpc_web_client::Client::new(self.grpc_addr.clone()))
    }

    /// Gets the information of a full node.
    pub async fn node_info(&self) -> Result<NodeInfo, CosmosError> {
        let endpoint = format!("{}{}", self.lcd_addr, "/node_info");
        let response = get(&endpoint)
            .await
            .map_err(|err| CosmosError::Lcd(err.to_string()))?;

        let node_info_response: NodeInfoResponse;
        match response.status() {
            StatusCode::OK => {
                // Unwrap here is safe since we already knew that the response is good
                node_info_response = response.json::<NodeInfoResponse>().await.unwrap()
            }
            status_code => return Err(CosmosError::Lcd(status_code.to_string())),
        }

        Ok(node_info_response.node_info)
    }

    /// Returns the account data associated to the given address.
    pub async fn get_account_data(&self, address: &str) -> Result<BaseAccount, CosmosError> {
        let mut client = QueryClient::new(self.grpc_client().await?);

        // Build a new request
        let request = Request::new(QueryAccountRequest {
            address: address.to_owned(),
        });

        // Send request and wait for response
        let response = client
            .account(request)
            .await
            .map_err(|err| CosmosError::Grpc(err.to_string()))?
            .into_inner();

        // Decode response body into BaseAccount
        let base_account: BaseAccount =
            prost::Message::decode(response.account.unwrap().value.as_ref())?;

        Ok(base_account)
    }

    /// Broadcast a tx using the gRPC interface.
    pub async fn broadcast_tx(
        &self,
        tx: &Tx,
        mode: BroadcastMode,
    ) -> Result<Option<TxResponse>, CosmosError> {
        // Some buffers used to serialize the objects
        let mut serialized_body: Vec<u8> = Vec::new();
        let mut serialized_auth: Vec<u8> = Vec::new();
        let mut serialized_tx: Vec<u8> = Vec::new();

        // Serialize the tx body and auth_info
        if let Some(body) = &tx.body {
            prost::Message::encode(body, &mut serialized_body)?;
        }
        if let Some(auth_info) = &tx.auth_info {
            prost::Message::encode(auth_info, &mut serialized_auth)?;
        }

        // Prepare and serialize the TxRaw
        let tx_raw = TxRaw {
            body_bytes: serialized_body,
            auth_info_bytes: serialized_auth,
            signatures: tx.signatures.clone(),
        };
        prost::Message::encode(&tx_raw, &mut serialized_tx)?;

        let mut service = ServiceClient::new(self.grpc_client().await?);

        let request = Request::new(BroadcastTxRequest {
            tx_bytes: serialized_tx,
            mode: mode as i32,
        });

        let response = service
            .broadcast_tx(request)
            .await
            .map_err(|e| CosmosError::Grpc(e.to_string()))?
            .into_inner();

        Ok(response.tx_response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::TxBuilder;
    use cosmos_sdk_proto::cosmos::{bank::v1beta1::MsgSend, base::v1beta1::Coin};
    use crw_wallet::crypto::MnemonicWallet;

    static TEST_MNEMONIC: &str = "elephant luggage finger obscure nest smooth flag clay recycle unfair capital category organ bicycle gallery sight canyon hotel dutch skull today pink scale aisle";
    static DESMOS_DERIVATION_PATH: &str = "m/44'/852'/0'/0/0";

    #[test]
    async fn node_info() {
        let cosmos_client =
            CosmosClient::new("http://localhost:1317", "http://localhost:9090").unwrap();

        let info = cosmos_client.node_info().await;

        assert!(info.is_ok());
        assert_eq!("testchain", info.unwrap().network);
    }

    #[test]
    async fn broadcast_tx() {
        let wallet = MnemonicWallet::new(TEST_MNEMONIC, DESMOS_DERIVATION_PATH).unwrap();

        let cosmos_client =
            CosmosClient::new("http://localhost:1317", "http://localhost:9090").unwrap();

        let address = wallet.get_bech32_address("desmos").unwrap();
        let account_data = cosmos_client.get_account_data(&address).await.unwrap();

        let amount = Coin {
            denom: "stake".to_string(),
            amount: "10".to_string(),
        };

        let msg_snd = MsgSend {
            from_address: address,
            to_address: "desmos18ek6mnlxj8sysrtvu60k5zj0re7s5n42yncner".to_string(),
            amount: vec![amount],
        };

        let tx = TxBuilder::new("testchain")
            .memo("Test memo")
            .account_info(account_data.sequence, account_data.account_number)
            .timeout_height(0)
            .fee("stake", "5000", 300_000)
            .add_message("/cosmos.bank.v1beta1.Msg/Send", msg_snd)
            .unwrap()
            .sign(&wallet)
            .unwrap();

        let res = cosmos_client
            .broadcast_tx(&tx, BroadcastMode::Block)
            .await
            .unwrap()
            .unwrap();

        print!("{}", res.raw_log);
        assert_eq!(0, res.code);
    }
}
