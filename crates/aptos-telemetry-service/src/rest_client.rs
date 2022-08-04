use anyhow::{anyhow, Result};
use aptos_rest_client::{Client, Response, state::State};
use aptos_types::{
    account_address::AccountAddress, account_config::CORE_CODE_ADDRESS, network_address::NetworkAddress, PeerId
};
use serde::de::DeserializeOwned;
use url::Url;

use crate::types::validator_set::{ValidatorSet,ValidatorInfo, ValidatorConfig};

#[derive(Clone)]
pub struct RestClient {
    client: Client,
}

impl RestClient {
    pub fn new(api_url: Url) -> Self {
        Self {
            client: Client::new(api_url),
        }
    }

    async fn get_resource<T: DeserializeOwned>(
        &self,
        address: AccountAddress,
        resource_type: &str,
    ) -> Result<Response<T>> {
        return self.client.get_resource(address, resource_type).await;
    }

    pub async fn validator_set(&self) -> Result<(Vec<ValidatorInfo>, State)> {
        let (validator_set, state): (ValidatorSet, State) = self
            .get_resource(CORE_CODE_ADDRESS, "0x1::stake::ValidatorSet")
            .await?
            .into_parts();

        let mut validator_infos = vec![];
        for validator_info in validator_set.payload() {
            validator_infos.push(validator_info.clone());
        }

        if validator_infos.is_empty() {
            return Err(anyhow!("No validator sets were found!"));
        }
        Ok((validator_infos, state))
    }


    pub async fn validator_set_validator_addresses(
        &self,
    ) -> Result<(Vec<(PeerId, Vec<NetworkAddress>)>, State)> {
        self.validator_set_addresses(|info| Self::validator_addresses(info.config()))
            .await
    }

    fn validator_addresses(config: &ValidatorConfig) -> Result<Vec<NetworkAddress>> {
        config
            .validator_network_addresses()
            .map_err(|e| anyhow!("unable to parse network address {}", e.to_string()))
    }

    async fn validator_set_addresses<F: Fn(ValidatorInfo) -> Result<Vec<NetworkAddress>>>(
        &self,
        address_accessor: F,
    ) -> Result<(Vec<(PeerId, Vec<NetworkAddress>)>, State)> {
        let (set, state) = self.validator_set().await?;
        let mut decoded_set = Vec::new();
        for info in set {
            let peer_id = *info.account_address();
            let addrs = address_accessor(info)?;
            decoded_set.push((peer_id, addrs));
        }

        Ok((decoded_set, state))
    }
}
