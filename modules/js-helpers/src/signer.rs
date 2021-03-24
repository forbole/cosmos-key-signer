use cosmos_sdk_proto::cosmos::tx::v1beta1::{BroadcastMode, Fee};
use crw_client::{client::get_node_info, client::ChainClient};
use crw_types::msg::{AnyWrapper, Msg};
use crw_wallet::{crypto::Wallet, crypto::WalletJS};
use prost_types::Any;
use wasm_bindgen::prelude::*;

/// Sign and send a transaction with the given wallet
#[wasm_bindgen]
#[allow(dead_code)]
pub async fn sign_and_send_msg(
    js_wallet: JsValue,
    js_msg: JsValue,
    js_fees: JsValue,
    memo: String,
    lcd_addr: String,
    grpc_addr: String,
) -> Result<JsValue, JsValue> {
    // convert all the js values to actual rust types
    let wallet_js: WalletJS = js_wallet.into_serde().unwrap();
    let wallet = Wallet::from(wallet_js);
    let msg_wrapped: AnyWrapper = js_msg.into_serde().unwrap();
    let msg: Msg = Msg(Any::from(msg_wrapped));
    let fees: Fee = js_fees.into_serde().unwrap();

    let response = get_node_info(lcd_addr.to_string())
        .await
        .map_err(|error| JsValue::from_serde(&error).unwrap())?;

    let chain_client = ChainClient::new(
        response.node_info.clone(),
        lcd_addr.to_string(),
        grpc_addr.to_string(),
    );

    let account = chain_client
        .get_account_data(wallet.bech32_address.clone())
        .await
        .map_err(|error| JsValue::from_serde(&error).unwrap())?;

    let msgs = vec![msg];
    let signed_bytes = wallet
        .sign_tx(
            account,
            chain_client.node_info.network.clone(),
            msgs,
            fees,
            Some(memo.to_string()),
            0,
        )
        .map_err(|error| JsValue::from_serde(&error).unwrap())?;

    let result = chain_client
        .broadcast_tx(signed_bytes, BroadcastMode::Block)
        .await
        .map_err(|error| JsValue::from_serde(&error).unwrap())?;

    Ok(JsValue::from_serde(&result.txhash).unwrap())
}

/// Import a wallet from the given mnemonic
#[wasm_bindgen]
#[allow(dead_code)]
pub fn import_wallet(mnemonic: &str, derivation_path: &str, hrp: &str) -> Result<JsValue, JsValue> {
    let wallet: WalletJS =
        Wallet::from_mnemonic(mnemonic, derivation_path.to_string(), hrp.to_string())
            .map_err(|error| JsValue::from_serde(&error).unwrap())?
            .into();

    Ok(JsValue::from_serde(&wallet).unwrap())
}

#[cfg(test)]
mod test {
    use crate::signer::import_wallet;
    use crw_wallet::crypto::WalletJS;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    async fn import_wallet_works() {
        let js_wallet = import_wallet(
            "battle call once stool three mammal hybrid list sign field athlete amateur cinnamon eagle shell erupt voyage hero assist maple matrix maximum able barrel",
            "m/44'/852'/0'/0/0",
            "desmos"
        ).unwrap();

        let wallet_js: WalletJS = js_wallet.into_serde().unwrap();

        assert_eq!(
            wallet_js.bech32_address,
            "desmos1k8u92hx3k33a5vgppkyzq6m4frxx7ewnlkyjrh"
        );
        assert_eq!(
            wallet_js.public_key,
            "02f5bf794ef934cb419bb9113f3a94c723ec6c2881a8d99eef851fd05b61ad698d"
        )
    }
}