use aws_sdk_s3 as s3;
use dotenv::dotenv;

#[tokio::main]
async fn main() -> Result<(), s3::Error> {
    // Load environment variables from .env file
    dotenv().ok();

    // Create custom config for MinIO
    let config = aws_config::from_env()
        .endpoint_url(std::env::var("AWS_ENDPOINT_URL").unwrap())
        .credentials_provider(s3::config::Credentials::new(
            std::env::var("AWS_ACCESS_KEY_ID").unwrap(),
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap(),
            None,
            None,
            // FIXME: how not to hard-code the region?
            "us-east-1",
        ))
        .load()
        .await;

    let client = aws_sdk_s3::Client::new(&config);

    // Example: List all buckets
    let bucket_name = std::env::var("BUCKET_NAME").unwrap();
    client.create_bucket().bucket(bucket_name).send().await?;

    let resp = client.list_buckets().send().await?;
    println!("Buckets:");
    for bucket in resp.buckets() {
        println!("  {}", bucket.name().unwrap_or_default());
    }

    Ok(())
}
