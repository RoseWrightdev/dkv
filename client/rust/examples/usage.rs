use dkv::DkvClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Connecting to DKV server...");
    let client = DkvClient::connect("http://127.0.0.1:50051").await?;

    let key = "rust-example-key";
    let val = b"Hello DKV from robust Async Rust!".to_vec();

    println!("Storing value under key...");
    client.set(key, val.clone()).await?;

    println!("Retrieving value...");
    if let Some(retrieved_val) = client.get(key).await? {
        let string_val = String::from_utf8(retrieved_val)?;
        println!("Success! Retrieved value: \"{}\"", string_val);
    } else {
        println!("Key not found!");
    }

    println!("Deleting key...");
    client.delete(key).await?;

    println!("Verifying deletion...");
    let check = client.get(key).await?;
    println!("Key exists: {}", check.is_some());

    println!("Done!");
    Ok(())
}
