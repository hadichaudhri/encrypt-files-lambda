use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::operation::{
    delete_object::DeleteObjectError, get_object::GetObjectError, list_objects::ListObjectsError,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;

pub trait GetFile {
    async fn get_file(&self, bucket: &str, key: &str) -> Result<Vec<u8>, GetObjectError>;
}

pub trait DeleteFile {
    async fn delete_file(&self, bucket: &str, key: &str) -> Result<String, DeleteObjectError>;
}

pub trait PutFile {
    async fn put_file(&self, bucket: &str, key: &str, bytes: Vec<u8>) -> Result<String, String>;
}

pub trait ListFiles {
    async fn list_files(&self, bucket: &str) -> Result<Vec<String>, String>;
}

impl GetFile for S3Client {
    async fn get_file(&self, bucket: &str, key: &str) -> Result<Vec<u8>, GetObjectError> {
        tracing::info!("get file bucket {}, key {}", bucket, key);

        let output = self.get_object().bucket(bucket).key(key).send().await;

        return match output {
            Ok(response) => {
                let bytes = response.body.collect().await.unwrap().to_vec();
                tracing::info!("Object is downloaded, size is {}", bytes.len());
                Ok(bytes)
            }
            Err(err) => {
                let service_err = err.into_service_error();
                let meta = service_err.meta();
                tracing::error!(
                    "Error from aws when downloading: {}, key: {}",
                    meta.to_string(),
                    key
                );
                Err(service_err)
            }
        };
    }
}

impl DeleteFile for S3Client {
    async fn delete_file(&self, bucket: &str, key: &str) -> Result<String, DeleteObjectError> {
        tracing::info!("delete file bucket {}, key {}", bucket, key);

        let res = self.delete_object().bucket(bucket).key(key).send().await;

        return match res {
            Ok(_) => Ok(format!("Deleted a file with key {} from {}", key, bucket)),
            Err(err) => {
                let service_err = err.into_service_error();
                let meta = service_err.meta();
                tracing::error!(
                    "Error from aws when deleting: {}, key: {}",
                    meta.to_string(),
                    key
                );
                Err(service_err)
            }
        };
    }
}

impl PutFile for S3Client {
    async fn put_file(&self, bucket: &str, key: &str, vec: Vec<u8>) -> Result<String, String> {
        tracing::info!("put file bucket {}, key {}", bucket, key);
        let bytes = ByteStream::from(vec);
        let result = self
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(bytes)
            .send()
            .await;

        match result {
            Ok(_) => Ok(format!("Uploaded a file with key {} into {}", key, bucket)),
            Err(err) => Err(err
                .into_service_error()
                .meta()
                .message()
                .unwrap_or_else(|| "Unknown upload error")
                .to_string()),
        }
    }
}

fn object_to_keys(objects: ListObjectsV2Output) -> Vec<String> {
    objects
        .contents()
        .into_iter()
        .filter_map(|obj| obj.key().map(|s| s.to_string()))
        .collect()
}

impl ListFiles for S3Client {
    async fn list_files(&self, bucket: &str) -> Result<Vec<String>, String> {
        tracing::info!("listing files from bucket {}", bucket);

        let res = self.list_objects_v2().bucket(bucket).send().await;

        match res {
            Ok(objects) => Ok(object_to_keys(objects)),
            Err(err) => Err(err
                .into_service_error()
                .meta()
                .message()
                .unwrap_or_else(|| "Unknown bucket access error")
                .to_string()),
        }
    }
}
