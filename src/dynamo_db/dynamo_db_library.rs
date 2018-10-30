use rusoto_core::Region;
use rusoto_dynamodb::*;
use std::collections::HashMap;

pub struct DynamoDbLibrary {
    dynamo_db_client: DynamoDbClient,
    table_name: String,
}

impl DynamoDbLibrary {
    pub fn new(table_name: String) -> DynamoDbLibrary {
        let dynamo_db_client =
            DynamoDbLibrary::initialize_new_dynamo_db_client();

        DynamoDbLibrary { dynamo_db_client, table_name }
    }

    fn initialize_new_dynamo_db_client() -> DynamoDbClient {
        return DynamoDbClient::new(Region::EuWest1);
    }

    /// Get the shard sequence number if it's already owned (has owner id).
    pub fn get_owned_shard_sequence_number_given_shard_id(&self, shard_id: &String) -> Option<String> {
        let db_record = self.get_db_full_record_using_shard_id(shard_id);
        if db_record.is_none() {
            return None;
        }

        let item_output = db_record.unwrap();
        if item_output.item.is_none() {
            return None;
        }

        let item = item_output.item.unwrap();
        let owner_id_attribute = item.get("owner_id");
        let sequence_number_attribute = item.get("sequence_number");

        if sequence_number_attribute.is_some() && owner_id_attribute.is_some() {
            let sequence_number_dynamo_option = sequence_number_attribute.unwrap().clone().s;
            let owner_id_dynamo_option = owner_id_attribute.unwrap().clone().s;

            if sequence_number_dynamo_option.is_some() && owner_id_dynamo_option.is_some() {
                return Some(sequence_number_dynamo_option.unwrap());
            }
        }

        return None;
    }

    /// Get the shard owner id.
    pub fn get_shard_owned_id(&self, shard_id: &String) -> Option<String> {
        let db_record = self.get_db_full_record_using_shard_id(shard_id);
        let item = db_record.unwrap().item.unwrap();
        let owner_id = item.get("owner_id");

        if owner_id.is_some() {
            let owner_id_string = owner_id.unwrap().clone().s;
            if owner_id_string.is_some() {
                return Some(owner_id_string.unwrap());
            }
        }

        return None;
    }

    pub fn extract_sequence_number_from_record(&self,
                                               item_option: Option<HashMap<String, AttributeValue>>)
                                               -> Option<String> {
        let item = item_option.unwrap();
        let sequence_number_attribute = item.get("sequence_number");
        if sequence_number_attribute.is_some() {
            let sequence_number_dynamo_option = sequence_number_attribute.unwrap().clone().s;
            if sequence_number_dynamo_option.is_some() {
                return Some(sequence_number_dynamo_option.unwrap());
            }
        }

        return None;
    }

    /// Update the owner of the shard to no owner.
    pub fn release_shard_from_owner(&self, shard_id: &String) {
        println!("Shard {} will be released from owner.", shard_id);
        let shard_id_attribute_value =
            self.get_string_attribute_value(shard_id.to_string());

        let mut item_input_hash_map = HashMap::new();
        item_input_hash_map.insert("shard_id".to_string(), shard_id_attribute_value);

        let get_item_input = self.get_shard_owner_reset_update_item_input(item_input_hash_map);
        let get_item_result = self.dynamo_db_client.update_item(get_item_input).sync();

        if get_item_result.is_err() {
            println!("Error while writing to Dynamo. {}", get_item_result.err().unwrap());
        }
    }

    pub fn get_db_full_record_using_shard_id(&self, shard_id: &String) -> Option<GetItemOutput> {
        let index_key: &str = "shard_id";
        let attribute_value = self.get_string_attribute_value(shard_id.to_string());

        let mut item_input_hash_map = HashMap::new();
        item_input_hash_map.insert(index_key.to_string(), attribute_value);
        let get_item_input = GetItemInput {
            attributes_to_get: None,
            consistent_read: None,
            expression_attribute_names: None,
            key: item_input_hash_map,
            projection_expression: None,
            return_consumed_capacity: None,
            table_name: self.table_name.to_string()
        };

        let item_output = self.dynamo_db_client.get_item(get_item_input).sync();
        if item_output.is_err() {
            println!("Error while reading from Dynamo. {}", item_output.err().unwrap());
            return None;
        }

        return Some(item_output.unwrap());
    }

    pub fn add_new_shard_record(&self, shard_id: &String, worker_id: &String) -> bool {
        let shard_id_attribute_value = self.get_string_attribute_value(shard_id.to_string());
        let owner_id_attribute_value = self.get_string_attribute_value(worker_id.to_string());
        let number_of_owners_switched_attribute_value =
            self.get_number_attribute_value("1".to_string());

        let mut item_input_hash_map = HashMap::new();
        item_input_hash_map.insert("shard_id".to_string(), shard_id_attribute_value);
        item_input_hash_map.insert("owner_id".to_string(), owner_id_attribute_value);
        item_input_hash_map.insert(
            "number_of_owners_switched".to_string(),
            number_of_owners_switched_attribute_value
        );

        let put_item_input = self.get_put_item_input(item_input_hash_map);
        let put_item_result =
            self.dynamo_db_client.put_item(put_item_input).sync();

        if put_item_result.is_err() {
            println!("Error while writing to Dynamo. {}", put_item_result.err().unwrap());
            return false;
        }

        return true;
    }

    pub fn update_shard_owner(&self, shard_id: &String, worker_id: &String) -> bool {
        let shard_id_attribute_value = self.get_string_attribute_value(shard_id.to_string());

        let mut item_input_hash_map = HashMap::new();
        item_input_hash_map.insert("shard_id".to_string(), shard_id_attribute_value);

        let get_item_input = self.get_shard_owner_update_item_input(item_input_hash_map, worker_id.to_string());
        let get_item_result = self.dynamo_db_client.update_item(get_item_input).sync();
        if get_item_result.is_err() {
            println!("Error while writing to Dynamo. {}", get_item_result.err().unwrap());
            return false;
        }

        return true;
    }

    pub fn update_shard_sequence_number(&self, shard_id: &String, sequence_number: &String) -> bool {
        let shard_id_attribute_value = self.get_string_attribute_value(shard_id.to_string());
        let mut item_input_hash_map = HashMap::new();
        item_input_hash_map.insert("shard_id".to_string(), shard_id_attribute_value);

        let update_item_input =
            self.get_sequence_number_update_item_input(
                item_input_hash_map, sequence_number
            );

        let put_item_result = self.dynamo_db_client.update_item(update_item_input).sync();
        if put_item_result.is_err() {
            println!("Error while writing to Dynamo. {}", put_item_result.err().unwrap());
            return false;
        }

        return true;
    }

    fn get_shard_owner_reset_update_item_input(&self, hash_map: HashMap<String, AttributeValue>)
                                               -> UpdateItemInput {
        let owner_id_attribute_value = self.get_null_attribute_value();
        let mut expression_attribute_values = HashMap::new();
        expression_attribute_values.insert(":owner_id_val".to_string(), owner_id_attribute_value);

        return UpdateItemInput {
            attribute_updates: None,
            condition_expression: None,
            conditional_operator: None,
            expected: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(expression_attribute_values),
            key: hash_map,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values: None,
            table_name: self.table_name.to_string(),
            update_expression: Some("SET owner_id = :owner_id_val".to_string())
        };
    }

    fn get_shard_owner_update_item_input(&self,
                                         hash_map: HashMap<String, AttributeValue>,
                                         owner_id: String) -> UpdateItemInput {
        let owner_id_attribute_value = self.get_string_attribute_value(owner_id);
        let number_of_owners_increment_attribute_value = self.get_number_attribute_value("1".to_string());
        let null_attribute_type = self.get_null_attribute_value();

        let mut expression_attribute_values = HashMap::new();
        expression_attribute_values.insert(
            ":owner_id_val".to_string(),
            owner_id_attribute_value
        );

        expression_attribute_values.insert(
            ":number_of_owners_increment_value".to_string(),
            number_of_owners_increment_attribute_value
        );

        expression_attribute_values.insert(
            ":null_attribute_type".to_string(),
            null_attribute_type
        );

        return UpdateItemInput {
            attribute_updates: None,
            condition_expression: Some("owner_id = :null_attribute_type".to_string()),
            conditional_operator: None,
            expected: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(expression_attribute_values),
            key: hash_map,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values: None,
            table_name: self.table_name.to_string(),
            update_expression: Some("SET owner_id = :owner_id_val, number_of_owners_switched = number_of_owners_switched + :number_of_owners_increment_value".to_string())
        };
    }

    fn get_string_attribute_value(&self, value: String) -> AttributeValue {
        return AttributeValue {
            b: None,
            bool: None,
            bs: None,
            l: None,
            m: None,
            n: None,
            ns: None,
            null: None,
            s: Some(value),
            ss: None
        };
    }

    fn get_null_attribute_value(&self) -> AttributeValue {
        return AttributeValue {
            b: None,
            bool: None,
            bs: None,
            l: None,
            m: None,
            n: None,
            ns: None,
            null: Some(true),
            s: None,
            ss: None
        };
    }

    fn get_number_attribute_value(&self, value: String) -> AttributeValue {
        return AttributeValue {
            b: None,
            bool: None,
            bs: None,
            l: None,
            m: None,
            n: Some(value),
            ns: None,
            null: None,
            s: None,
            ss: None
        };
    }

    fn get_put_item_input(&self, hash_map: HashMap<String, AttributeValue>) -> PutItemInput {
        return PutItemInput {
            condition_expression: None,
            conditional_operator: None,
            expected: None,
            expression_attribute_names: None,
            expression_attribute_values: None,
            item: hash_map,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values: None,
            table_name: self.table_name.to_string()
        };
    }

    fn get_sequence_number_update_item_input(&self, hash_map: HashMap<String, AttributeValue>,
                                             sequence_number: &String) -> UpdateItemInput {
        let sequence_number_attribute_value =
            self.get_string_attribute_value(sequence_number.to_string());

        let mut expression_attribute_values = HashMap::new();
        expression_attribute_values.insert(
            ":sequence_number_val".to_string(), sequence_number_attribute_value
        );

        return UpdateItemInput {
            attribute_updates: None,
            condition_expression: None,
            conditional_operator: None,
            expected: None,
            expression_attribute_names: None,
            expression_attribute_values: Some(expression_attribute_values),
            key: hash_map,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values: None,
            table_name: self.table_name.to_string(),
            update_expression: Some("SET sequence_number = :sequence_number_val".to_string())
        };
    }
}
