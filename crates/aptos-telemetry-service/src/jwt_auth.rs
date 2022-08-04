// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use aptos_config::config::PeerRole;
use aptos_types::{chain_id::ChainId, PeerId};
use chrono::Utc;
use jsonwebtoken::{decode, encode, errors::Error, Algorithm, Header, Validation};
use warp::{
    http::header::{HeaderMap, HeaderValue, AUTHORIZATION},
    reject, Rejection,
};

use crate::{context::Context, types::auth::Claims};

const BEARER: &str = "BEARER: ";

pub fn create_jwt_token(
    context: Context,
    chain_id: ChainId,
    peer_id: PeerId,
    peer_role: PeerRole,
    epoch: u64,
) -> Result<String, Error> {
    let issued = Utc::now().timestamp();
    let expiration = Utc::now()
        .checked_add_signed(chrono::Duration::minutes(60))
        .expect("valid timestamp")
        .timestamp();

    let claims = Claims {
        chain_id,
        peer_id,
        peer_role,
        epoch,
        exp: expiration as usize,
        iat: issued as usize,
    };
    let header = Header::new(Algorithm::HS512);
    encode(&header, &claims, &context.jwt_encoding_key)
}

pub async fn authorize_jwt(
    token: String,
    (context, allow_roles): (Context, Vec<PeerRole>),
) -> anyhow::Result<Claims, Rejection> {
    let decoded = decode::<Claims>(
        &token,
        &context.jwt_decoding_key,
        &Validation::new(Algorithm::HS512),
    )
    .map_err(|_| reject::reject())?;

    let claims = decoded.claims;

    let current_epoch = context
        .validator_cache()
        .read()
        .get(&claims.chain_id)
        .unwrap()
        .0;

    if allow_roles.contains(&claims.peer_role)
        && claims.epoch == current_epoch
        && claims.exp < Utc::now().timestamp() as usize
    {
        Ok(claims)
    } else {
        Err(reject::reject())
    }
}

pub async fn jwt_from_header(headers: HeaderMap<HeaderValue>) -> anyhow::Result<String, Rejection> {
    let header = match headers.get(AUTHORIZATION) {
        Some(v) => v,
        None => return Err(reject::reject()),
    };
    let auth_header = match std::str::from_utf8(header.as_bytes()) {
        Ok(v) => v,
        Err(_) => return Err(reject::reject()),
    };
    if !auth_header.starts_with(BEARER) {
        return Err(reject::reject());
    }
    Ok(auth_header.trim_start_matches(BEARER).to_owned())
}
