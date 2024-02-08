use chacha20poly1305::{
    aead::{Aead, OsRng},
    KeyInit, XChaCha20Poly1305,
};
use rand::RngCore;
pub use s3_client::{DeleteFile, GetFile, ListFiles, PutFile};

mod s3_client;

pub fn get_encrypted_file(
    file_data: Vec<u8>,
    key: &[u8; 32],
    nonce: &[u8; 24],
) -> Result<Vec<u8>, String> {
    let cipher = XChaCha20Poly1305::new(key.into());

    let encrypted_file = cipher
        .encrypt(nonce.into(), file_data.as_ref())
        .map_err(|err| format!("Encrypting small file: {}", err))?;

    Ok(encrypted_file)
}

pub fn gen_encryption_config() -> ([u8; 32], [u8; 24]) {
    let mut enc_key = [0u8; 32];
    let mut nonce = [0u8; 24];
    OsRng.fill_bytes(&mut enc_key);
    OsRng.fill_bytes(&mut nonce);
    (enc_key, nonce)
}
