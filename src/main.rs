use aws_lambda_events::s3::S3EventRecord;
use aws_sdk_s3::Client as S3Client;
use chacha20poly1305::{aead::Aead, KeyInit, XChaCha20Poly1305};
use dotenv::dotenv;
use encrypt_files::{DeleteFile, GetFile, ListFiles, PutFile};
use lambda_http::{run, service_fn, Body, Error, Request, Response};
use rand::{rngs::OsRng, RngCore};
use std::env;

/**
This lambda handler is activated upon a HTTP request to the lambda URL.
When activited, it scans for all files in [original bucket name] and for each file:
    * downloads the created file
    * creates a encrypted file from it
    * uploads the encrypted to bucket "[original bucket name]-encrypted".
    * deletes the original unencrypted file

When deploying, ensure that
    * the created files have no strange characters in the name
    * there is another bucket with "-encrypted" suffix in the name
    * this lambda only gets events from file creation
    * this lambda has permission to put file into the "-encrypted" bucket
    * this lambda has permission to delete files from the unencrypted bucket
*/
pub(crate) async fn function_handler<T: PutFile + GetFile + DeleteFile + ListFiles>(
    req: Request,
    client: &T,
) -> Result<Response<Body>, Error> {
    dotenv().ok();

    let mut enc_key = [0u8; 32];
    let mut nonce = [0u8; 24];
    OsRng.fill_bytes(&mut enc_key);
    OsRng.fill_bytes(&mut nonce);

    let bucket = env::var("BUCKET_NAME").expect("BUCKET_NAME must be set.");

    let keys = match client.list_files(&bucket).await {
        Ok(k) => k,
        Err(e) => {
            tracing::error!("Can not list files from bucket");
            return Err(e.into());
        }
    };

    let mut encountered_error = false;
    for key in keys {
        let file = match client.get_file(&bucket, &key).await {
            Ok(data) => data,
            Err(msg) => {
                tracing::error!("Can not get file from S3: {}", msg);
                encountered_error = true;
                continue;
            }
        };

        let enc_file = match get_encrypted_file(file, &enc_key, &nonce) {
            Ok(vec) => vec,
            Err(msg) => {
                tracing::error!("Can not create encrypted file: {}", msg);
                encountered_error = true;
                continue;
            }
        };

        tracing::info!(
            "Successfully encrypted file {} with encryption key {} and nonce {}",
            key,
            hex::encode(enc_key),
            hex::encode(nonce)
        );

        let mut encrypted_bucket = bucket.to_owned();
        encrypted_bucket.push_str("-encrypted");

        match client.put_file(&encrypted_bucket, &key, enc_file).await {
            Ok(msg) => tracing::info!(msg),
            Err(msg) => {
                tracing::error!("Can not upload encrypted file: {}", msg);
                encountered_error = true;
            }
        }

        match client.delete_file(&bucket, &key).await {
            Ok(msg) => tracing::info!(msg),
            Err(msg) => {
                tracing::error!("Can not delete unencrypted file: {}", msg);
                encountered_error = true;
            }
        }
    }

    let message = if !encountered_error {
        format!(
            "Successfully encrypted all files with enc_key={} and nonce={}",
            hex::encode(enc_key),
            hex::encode(nonce)
        )
    } else {
        "Encountered errors while processing files! Please contact the developer for more details!"
            .to_string()
    };

    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(message.into())
        .map_err(Box::new)?;
    Ok(resp)
}

fn get_file_props(record: S3EventRecord) -> Result<(String, String), String> {
    record
        .event_name
        .filter(|s| s.starts_with("ObjectCreated"))
        .ok_or("Wrong event")?;

    let bucket = record
        .s3
        .bucket
        .name
        .filter(|s| !s.is_empty())
        .ok_or("No bucket name")?;

    let key = record
        .s3
        .object
        .key
        .filter(|s| !s.is_empty())
        .ok_or("No object key")?;

    Ok((bucket, key))
}

fn get_encrypted_file(
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    // required to enable CloudWatch error logging by the runtime
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    let shared_config = aws_config::load_from_env().await;
    let client = S3Client::new(&shared_config);
    let client_ref = &client;

    let func = service_fn(move |req| async move { function_handler(req, client_ref).await });

    run(func).await?;

    Ok(())
}
