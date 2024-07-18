# Axum Session for Dynamodb database

Specify environment variables
```bash
AWS_ACCESS_KEY_ID=*******************
AWS_SECRET_ACCESS_KEY=*******************
AWS_REGION=*************
AWS_ENDPOINT_URL=*******************
```

Usage example:

```rust
    let shared_config = aws_config::load_from_env().await;
    let client = DynamoDbClient::new(&shared_config);
    
    let session_config = SessionConfig::default().with_table_name("sessions");
    let session_store = SessionStore::<SessionDynamoDbPool>::new(Some(pool), session_config).await.unwrap();

    ...


    let app = Router::new()
            ...
            .layer(axum_session::SessionLayer::new(session_store));
```