use async_trait::async_trait;
use aws_sdk_dynamodb::types::{AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType, Select, TimeToLiveSpecification};
use axum_session::{DatabaseError, DatabasePool};

use crate::DynamoDbClient;

#[derive(Clone, Debug)]
pub struct SessionDynamoDbPool {
    client: DynamoDbClient,
}

impl From<DynamoDbClient> for SessionDynamoDbPool {
    fn from(client: DynamoDbClient) -> Self {
        Self{
            client
        }
    }
}

impl SessionDynamoDbPool {
  
    async fn create_tables(&self, table_name: &str)->Result<(), aws_sdk_dynamodb::Error> {
        let res = self
        .client
        .list_tables()
        .send()
        .await?;


        if res
            .table_names
            .unwrap_or_default()
            .into_iter()
            .any(|s| s == table_name)
        {
            return Ok(());
        }

        let id: &str = "id";
        let session: &str = "session";
        let expires: &str = "expires";

        let id_ad = AttributeDefinition::builder()
            .attribute_name(id)
            .attribute_type(ScalarAttributeType::S)
            .build()?;

        let session_ad = AttributeDefinition::builder()
            .attribute_name(session)
            .attribute_type(ScalarAttributeType::S)
            .build()?;

        let expires_ad = AttributeDefinition::builder()
            .attribute_name(expires)
            .attribute_type(ScalarAttributeType::N)
            .build()?;

        let ks = KeySchemaElement::builder()
            .attribute_name(id)
            .key_type(KeyType::Hash)
            .build()?;

        self.client
            .create_table()
            .table_name(table_name)
            .key_schema(ks)
            .attribute_definitions(id_ad)
            .attribute_definitions(session_ad)
            .attribute_definitions(expires_ad)
            .send()
            .await?;

        self.client
            .update_time_to_live()
            .table_name(table_name)
            .time_to_live_specification(
                TimeToLiveSpecification::builder()
                    .enabled(true)
                    .attribute_name("expires")
                    .build()?,
            )
            .send()
            .await?;

            Ok(())
    }
}

#[async_trait]
impl DatabasePool for SessionDynamoDbPool{
     /// This is called to create the table in the database using the given table name.
    /// if an error occurs it should be propagated to the caller.
    async fn initiate(&self, table_name: &str) -> Result<(), DatabaseError> {
        
        self.create_tables(table_name).await
            .map_err(|e| DatabaseError::GenericCreateError(e.to_string()))

    }

    /// This is called to receive the session count in the database using the given table name.
    /// if an error occurs it should be propagated to the caller.
    async fn count(&self, table_name: &str) -> Result<i64, DatabaseError> {
        let res = self
            .client
            .query()
            .select(Select::Count)
            .table_name(table_name)
            .send()
            .await
            .map_err(|e| DatabaseError::GenericSelectError(e.to_string()))?;

        Ok(res.count.into())
    }

    /// This is called to store a session in the database using the given table name.
    /// The session is a string and should be stored in its own field.
    /// if an error occurs it should be propagated to the caller.
    /// expires is a unix timestamp(number of non-leap seconds since January 1, 1970 0:00:00 UTC)
    /// which is set to UTC::now() + the expiration time.
    async fn store(
        &self,
        id: &str,
        session: &str,
        expires: i64,
        table_name: &str,
    ) -> Result<(), DatabaseError> {
        self.client
            .put_item()
            .table_name(table_name)
            .item("id", AttributeValue::S(id.into()))
            .item("session", AttributeValue::S(session.into()))
            .item("expires", AttributeValue::N(expires.to_string()))
            .send()
            .await
            .map_err(|e| DatabaseError::GenericInsertError(e.to_string()))?;
        Ok(())
    }

    /// This is called to receive the session from the database using the given table name.
    /// if an error occurs it should be propagated to the caller.
    async fn load(&self, id: &str, table_name: &str) -> Result<Option<String>, DatabaseError> {
        let output = self
            .client
            .get_item()
            .table_name(table_name)
            .key("id", AttributeValue::S(id.into()))
            .send()
            .await
            .map_err(|e| DatabaseError::GenericSelectError(e.to_string()))?;

        Ok(output
            .item
            .map(|i| i["session"].as_s().ok().cloned())
            .flatten())
    }

    /// This is called to delete one session from the database using the given table name.
    /// if an error occurs it should be propagated to the caller.
    async fn delete_one_by_id(&self, id: &str, table_name: &str) -> Result<(), DatabaseError> {
        self.client
            .delete_item()
            .table_name(table_name)
            .key("id", AttributeValue::S(id.into()))
            .send()
            .await
            .map_err(|e| DatabaseError::GenericDeleteError(e.to_string()))?;
        Ok(())
    }

    /// This is called to check if the id exists in the database using the given table name.
    /// if an error occurs it should be propagated to the caller.
    async fn exists(&self, id: &str, table_name: &str) -> Result<bool, DatabaseError> {
        let res = self
            .client
            .query()
            .table_name(table_name)
            .key_condition_expression("id = :id")
            .expression_attribute_values(":id", AttributeValue::S(id.into()))
            .select(Select::Count)
            .send()
            .await
            .map_err(|e| DatabaseError::GenericSelectError(e.to_string()))?;

        Ok(res.count > 0)
    }

    /// This is called to delete all sessions that expired from the database using the given table name.
    /// if an error occurs it should be propagated to the caller.
    async fn delete_by_expiry(&self, _table_name: &str) -> Result<Vec<String>, DatabaseError> {
        // Dynamodb does this for us
        Ok(vec![])
    }

    /// This is called to delete all sessions from the database using the given table name.
    /// if an error occurs it should be propagated to the caller.
    async fn delete_all(&self, table_name: &str) -> Result<(), DatabaseError> {
        self.client
            .delete_item()
            .table_name(table_name)
            .send()
            .await
            .map_err(|e| DatabaseError::GenericSelectError(e.to_string()))?;
        Ok(())
    }

    /// This is called to get all id's in the database from the last run.
    /// if an error occurs it should be propagated to the caller.
    async fn get_ids(&self, table_name: &str) -> Result<Vec<String>, DatabaseError> {
        let res = self
        .client
        .query()
        .table_name(table_name)            
        .projection_expression("id")    
        .select(Select::SpecificAttributes)        
        .send()
        .await
        .map_err(|e| DatabaseError::GenericSelectError(e.to_string()))?;

        let ids = res.items.map(|items|
            items.into_iter()
                .map(|item|
                    item["id"].as_s().ok().cloned()
                ).flatten().collect()
        ).unwrap_or_default();
        Ok(ids)
    }

    fn auto_handles_expiry(&self) -> bool {
        true
    }
}
