mod session_pool;

pub type DynamoDbClient = aws_sdk_dynamodb::Client;
pub use session_pool::*;
