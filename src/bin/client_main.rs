use mock_gmp_api::Client;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenv::dotenv().ok();
    let server_address = std::env::var("SERVER_ADDRESS").unwrap();
    let server_port = std::env::var("SERVER_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let client = Client::new(format!("http://{}:{}", server_address, server_port));

    let task = serde_json::json!(
    {
      "id": "0197a679-9cf6-785c-8666-a2cf0c84c984",
      "chain": "xrpl",
      "timestamp": "2025-06-25T09:44:37.366743Z",
      "type": "GATEWAY_TX",
      "meta": {
        "txID": null,
        "fromAddress": null,
        "finalized": null,
        "sourceContext": null,
        "scopedMessages": [
          {
            "messageID": "0xf8ce6ce5191d701d28ff70deb326392070aaffc19b348439a4b6e2d7b9223091-164275108",
            "sourceChain": "axelar"
          }
        ]
      },
      "task": {
        "executeData": "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"
      }
    });

    let events = serde_json::json!(
      {  "events": [
    {
        "type": "CALL",
        "eventID": "0xe168dcf7f0e7ce7c4676a71ee21abd2e8a78a5c6ac49706cc99a884d2000de54-call",
        "txID": "0xe168dcf7f0e7ce7c4676a71ee21abd2e8a78a5c6ac49706cc99a884d2000de54",
        "fromAddress": null,
        "finalized": null,
        "sourceContext": {
          "xrpl_message": "{\"interchain_transfer_message\":{\"tx_id\":\"e168dcf7f0e7ce7c4676a71ee21abd2e8a78a5c6ac49706cc99a884d2000de54\",\"source_address\":\"rB9Y8qCjWamxfdxBs2g4h2e4RNVkvQd3WD\",\"destination_chain\":\"xrpl-evm\",\"destination_address\":\"48f7C7cF01B5D8983d96a1311f4F76Dad7311Ca1\",\"payload_hash\":null,\"transfer_amount\":{\"drops\":7300000},\"gas_fee_amount\":{\"drops\":1700000}}}"
        },
        "timestamp": "2025-07-10T12:18:42Z",
        "message": {
          "messageID": "0xe168dcf7f0e7ce7c4676a71ee21abd2e8a78a5c6ac49706cc99a884d2000de54",
          "sourceChain": "xrpl",
          "sourceAddress": "rNrjh1KGZk2jBR3wPfAQnoidtFFYQKbQn2",
          "destinationAddress": "axelar1aqcj54lzz0rk22gvqgcn8fr5tx4rzwdv5wv5j9dmnacgefvd7wzsy2j2mr",
          "payloadHash": "73940153ab066fd16b1ce5aacffbe6c693b90d2fcd29e5927c0a06cce85f9e27"
        },
        "destinationChain": "axelar",
        "payload": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAYAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAh4cnBsLWV2bQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC6WiHKiO9ruiv/9QiJlPkOEHfiocw9zDi9Jh8A/OKCTwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAASAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG9joAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACJyQjlZOHFDaldhbXhmZHhCczJnNGgyZTRSTlZrdlFkM1dEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABRI98fPAbXYmD2WoTEfT3ba1zEcoQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      }
    ]
    }
    );

    match client.post_task(task).await {
        Ok(response) => println!("Success: {}", response),
        Err(e) => println!("Error: {}", e),
    }

    match client.get_tasks().await {
        Ok(response) => println!("Success: {}", response),
        Err(e) => println!("Error: {}", e),
    }

    match client.post_events(events).await {
        Ok(response) => println!("Success: {}", response),
        Err(e) => println!("Error: {}", e),
    }

    println!("Succesfully performed all calls");

    Ok(())
}
