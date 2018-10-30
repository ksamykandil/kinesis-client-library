use rusoto_core::Region;
use rusoto_core::ProvideAwsCredentials;
use rusoto_core::credential::{AwsCredentials, DefaultCredentialsProvider, ChainProvider};
use rusoto_kinesis::*;
use rusoto_sts::{StsClient, StsAssumeRoleSessionCredentialsProvider};
use dynamo_db::dynamo_db_library::DynamoDbLibrary;
use std::collections::HashMap;
use std::thread;
use std::time;
use std::time::{Duration, Instant};
use rusoto_s3::*;
use tokio_core::reactor;
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
use uuid::Uuid;

use hyper::*;
use hyper::header::HeaderValue;
use hyper::rt::{self, Future, Stream};
use hyper::client::HttpConnector;
use rusoto_core::request::HttpClient;
use rusoto_core::DispatchSignedRequest;
use std::sync::Arc;
use rusoto_credential::AutoRefreshingProvider;

const STREAM_NAME_STR: &str = "kinesis_stream_name";

pub struct KinesisStreamLibrary {
    stream_name: String,
    dynamo_db_library: DynamoDbLibrary,
    s3_client: S3Client,
    kinesis_client: Arc<KinesisClient>,
    is_debug_enabled: bool,
}

// TODO .. Update the code to handle if the iam_role_arn is given so it's a multi account setup, otherwise follow the normal AWS Credentials setup.
impl KinesisStreamLibrary {
    pub fn new(iam_role_arn: String, dynamo_db_library: DynamoDbLibrary,
               s3_client: S3Client, is_debug_enabled: bool) -> KinesisStreamLibrary {
        let region = Region::EuWest1;
        let sts = StsClient::new(region.clone());
        let provider =
            StsAssumeRoleSessionCredentialsProvider::new(
                sts,
                iam_role_arn.to_owned(),
                "default".to_owned(),
                None, None, None, None
            );

        let auto_refreshing_provider = AutoRefreshingProvider::new(provider);
        let kinesis_client = Arc::new(
            KinesisClient::new_with(HttpClient::new().unwrap(),
                                    auto_refreshing_provider.unwrap(),
                                    region.clone()
            ));

        KinesisStreamLibrary {
            stream_name: STREAM_NAME_STR.to_string(),
            dynamo_db_library,
            s3_client,
            kinesis_client,
            is_debug_enabled
        }
    }

    pub fn get_stream_shards(&self) -> Option<Vec<Shard>> {
        let mut all_stream_shards: Vec<Shard> = Vec::new();
        let mut has_more_shards = true;
        let mut exclusive_shard_id = None;

        while has_more_shards {
            let describe_stream_input =
                DescribeStreamInput {
                    exclusive_start_shard_id: exclusive_shard_id.clone(),
                    limit: None,
                    stream_name: self.stream_name.to_string()
                };

            let describe_stream_request =
                self.kinesis_client.describe_stream(describe_stream_input).sync();

            if describe_stream_request.is_err() {
                println!("{}",
                         format!(
                             "Couldn't describe stream. {}",
                             describe_stream_request.err().unwrap()
                         )
                );

                return None;
            }

            let describe_stream_output = describe_stream_request.unwrap();
            let mut stream_shards = describe_stream_output.stream_description.shards;

            has_more_shards = describe_stream_output.stream_description.has_more_shards.clone();
            let shards_len = stream_shards.len();
            if has_more_shards && shards_len > 0 {
                exclusive_shard_id = Some(stream_shards[shards_len - 1].shard_id.to_string());
            }

            all_stream_shards.append(&mut stream_shards);
        }

        return Some(all_stream_shards);
    }

    /// Release the shards that are idle, no one is reading from it.
    pub fn cut_the_loose_for_the_idle_shards(&self, shards: &Vec<Shard>) {
        let mut old_stream_shards_sequence_number_map = HashMap::new();

        for shard in shards {
            let shard_sequence_number =
                self.dynamo_db_library.get_owned_shard_sequence_number_given_shard_id(
                    &(shard.shard_id)
                );

            if shard_sequence_number.is_some() {
                old_stream_shards_sequence_number_map.insert(
                    shard.shard_id.to_string(),
                    shard_sequence_number.unwrap().to_string()
                );
            }
        }

        /// Wait for 5 minutes and re-check that the sequence number changed.
        /// If doesn't change for 5 minutes, this shard should be assigned to a new thread.
        let sleep_time = time::Duration::from_millis(300000);
        thread::sleep(sleep_time);

        // TODO .. check if the record is empty - shard owner with empty shard id.
        for shard in shards {
            let shard_sequence_number =
                self.dynamo_db_library.get_owned_shard_sequence_number_given_shard_id(
                    &(shard.shard_id)
                );

            if shard_sequence_number.is_some() {
                let old_sequence_number =
                    old_stream_shards_sequence_number_map.get(&shard.shard_id);

                let shard_sequence_number_string = shard_sequence_number.unwrap().to_string();
                if old_sequence_number.is_some() &&
                    shard_sequence_number_string == old_sequence_number.unwrap().to_string() {
                    self.dynamo_db_library.release_shard_from_owner(&shard.shard_id);
                }
            }
        }
    }

    pub fn read_from_given_shard(&self, worker_id: &String, shard_id: &String) {
        let mut sequence_number: Option<String> = None;
        let item_output =
            self.dynamo_db_library.get_db_full_record_using_shard_id(shard_id);

        if item_output.is_none() {
            println!("No records for shard {} - Adding one for Worker {}.", shard_id, worker_id);
            let added = self.dynamo_db_library.add_new_shard_record(shard_id, worker_id);
            if !added {
                println!("Can't add new shard record.");
                return;
            }
        } else {
            let item_option = item_output.unwrap().item;
            if item_option.is_none() {
                println!("Empty shard {} record - Adding one for Worker {}.", shard_id, worker_id);
                let added = self.dynamo_db_library.add_new_shard_record(shard_id, worker_id);
                if !added {
                    println!("Can't add new shard record.");
                    return;
                }
            } else {
                let item = item_option.clone().unwrap();
                let owner_id = item.get("owner_id");
                if owner_id.is_none() || owner_id.unwrap().clone().s.is_none() {
                    // No owner for this shard.
                    println!("Update shard owner for shard {} - Worker {}.", shard_id, worker_id);
                    let updated = self.dynamo_db_library.update_shard_owner(shard_id, worker_id);
                    if !updated {
                        println!("Shard {} owner can't be updated. ", shard_id);
                        return;
                    }
                } else {
                    println!("Shard {} is already owned.", shard_id);
                    return;
                }

                sequence_number = self.dynamo_db_library.extract_sequence_number_from_record(item_option.clone());
            }
        }

        self.initialize_shard_stream_processing(sequence_number, shard_id, worker_id);
    }

    fn initialize_shard_stream_processing(&self, sequence_number: Option<String>,
                                          shard_id: &String, worker_id: &String) {
        let is_valid = self.validate_shard_owner_with_current_thread(shard_id, worker_id);
        if !is_valid {
            println!("Owner {} is trying to read from an already owned shard.", worker_id.to_string());
            return;
        }

        let shard_iterator = self.get_shard_iterator_input(shard_id, sequence_number);
        let shard_iterator_output =
            self.kinesis_client
                .get_shard_iterator(shard_iterator)
                .sync()
                .expect("No Shard Iterator.");

        // Read records from Kinesis Stream
        self.read_from_kinesis_stream(
            shard_id,
            shard_iterator_output.shard_iterator.unwrap().to_string(),
            1000,
            worker_id
        );
    }

    fn read_from_kinesis_stream(&self, shard_id: &String, mut shard_iterator_string: String,
                                mut number_of_records_limit: i64, worker_id: &String) {
        let mut number_of_retries = 1;
        let mut number_of_reads = 0;

        let mut client = Client::new();
        let mut reactor = reactor::Core::new().unwrap();

        loop {
            let start_time = Instant::now();

            number_of_reads = number_of_reads + 1;
            if number_of_reads == 10 {
                number_of_reads = 0;
                let is_valid = self.validate_shard_owner_with_current_thread(shard_id, worker_id);
                if !is_valid {
                    println!("Owner {} is trying to read from an already owned shard.", worker_id);
                    return;
                }
            }

            let records_input = GetRecordsInput { limit: Some(number_of_records_limit), shard_iterator: (*shard_iterator_string).to_string() };
            let records_result = self.kinesis_client.get_records(records_input).sync();

            if records_result.is_err() {
                if self.is_debug_enabled {
                    println!(
                        "Error while reading {} records from stream. Number of retries: {} - {}",
                        number_of_records_limit, number_of_retries,
                        format!("{:?}", records_result.unwrap_err())
                    );
                }

                number_of_retries = number_of_retries + 1;
                if number_of_retries > 10 {
                    self.dynamo_db_library.release_shard_from_owner(shard_id);
                    return;
                }

                let back_off_time = self.get_back_off_milli(number_of_retries) * 100;
                let sleep_time = time::Duration::from_millis(back_off_time);
                thread::sleep(sleep_time);
            } else {
                if self.is_debug_enabled {
                    println!("Reading {} records from Kinesis stream Successfully.", number_of_records_limit);
                }

                number_of_retries = 1;
                let records = records_result.unwrap();
                let shard_iterator = records.next_shard_iterator;

                if records.records.len() > 0 {
                    let pushed = self.push_logs_to_s3_and_elastic_search(
                        &(records.records), &client, &mut reactor
                    );

                    if !pushed {
                        println!("Can't push logs to ES.");
                        return;
                    }

                    let mut sequence_number = None;
                    for record in records.records {
                        sequence_number = Some(record.sequence_number.to_string());
                    }

                    if sequence_number.is_some() {
                        self.dynamo_db_library.update_shard_sequence_number(
                            &shard_id, &(sequence_number.unwrap())
                        );
                    }
                }

                if shard_iterator.is_some() {
                    shard_iterator_string = shard_iterator.unwrap().to_string();
                } else {
                    println!("No more records in shard {}.", shard_id.to_string());
                    return;
                }
            }
        }
    }

    pub fn get_back_off_milli(&self, number_of_retries: u32) -> u64 {
        if self.is_debug_enabled {
            println!("Calculating the exponential back_off");
        }

        let two: u64 = 2;
        return two.pow(number_of_retries);
    }

    fn validate_shard_owner_with_current_thread(&self, shard_id: &String, worker_id: &String) -> bool {
        let _owner_id = self.dynamo_db_library.get_shard_owned_id(shard_id);
        if _owner_id.is_none() {
            return false;
        }

        return _owner_id.unwrap().to_string() == worker_id.to_string();
    }

    fn get_shard_iterator_input(&self, shard_id: &String, sequence_number: Option<String>)
                                -> GetShardIteratorInput {
        if sequence_number.is_some() {
            return GetShardIteratorInput {
                shard_id: shard_id.to_string(),
                shard_iterator_type: "AT_SEQUENCE_NUMBER".to_string(),
                starting_sequence_number: sequence_number,
                stream_name: self.stream_name.to_string(),
                timestamp: None
            };
        }

        return GetShardIteratorInput {
            shard_id: shard_id.to_string(),
            shard_iterator_type: "TRIM_HORIZON".to_string(),
            starting_sequence_number: None,
            stream_name: self.stream_name.to_string(),
            timestamp: None
        };
    }

    /// Hourly index.
    fn push_logs_to_s3_and_elastic_search(&self, docs: &Vec<Record>,
                                          client: &Client<HttpConnector, Body>,
                                          reactor: &mut reactor::Core) -> bool {
        let date = Utc::now();
        let index_name = format!("{}_{}", "index_name", date.format("%Y_%m_%d_%H"));
        let mut batch: Vec<String> = vec![];

        for doc in docs {
            let record = (*doc).clone().data;
            let doc_string = String::from_utf8(record).unwrap().to_string();
            batch.push(format!("{{\"index\": {{\"_index\": \"{}\", \"_type\": \"_doc\"}} }}", index_name).to_string());
            batch.push(doc_string);
        }

        let bulk = batch.join("\n") + "\n";
        let mut pushed = false;
        while !pushed {
            pushed = false;
            let logs_pushed_to_s3 = self.push_logs_to_s3(bulk.clone());
            if logs_pushed_to_s3 {
                pushed = true;

                let sleep_time = time::Duration::from_millis(1000);
                thread::sleep(sleep_time);
            }
        }

        let mut number_of_retrials = 0;
        let mut client = Client::new();
        let mut reactor = reactor::Core::new().unwrap();
        let mut pushed = false;
        while !pushed && number_of_retrials <= 5 {
            number_of_retrials = number_of_retrials + 1;
            let elasticsearch_url = "http://localhost:8081/_bulk";
            let uri: hyper::Uri = elasticsearch_url.parse().unwrap();
            let mut req = hyper::Request::new(Body::from(bulk.clone()));
            *req.method_mut() = Method::POST;
            *req.uri_mut() = uri.clone();
            req.headers_mut().insert("content-type", HeaderValue::from_str("application/json").unwrap());
            let future = client.request(req);
            let ret = reactor.run(future);

            if self.is_debug_enabled {
                println!("pushing to elastic search. - {}", format!("{:?}", ret));
            }

            if ret.is_err() {
                println!("Error while pushing to Elastic search. - {}", ret.unwrap_err());

                let sleep_time = time::Duration::from_millis(1000);
                thread::sleep(sleep_time);
            } else {
                pushed = true;
            }
        }

        return pushed;
    }

    /// Save the logs to S3 to a second granularity.
    fn push_logs_to_s3(&self, log_messages: String) -> bool {
        let vector = log_messages.as_bytes().to_vec();
        let date = Utc::now();
        let file_path = format!("{}_{}.json", date.format("%Y/%m/%d/%H/%M/%S"), Uuid::new_v4());
        let s3_bucket_name = "s3_bucket_name";

        let response = self.s3_client.put_object(
            PutObjectRequest {
                acl: None,
                body: Some(StreamingBody::from(vector)),
                bucket: s3_bucket_name.to_string(),
                cache_control: None,
                content_disposition: None,
                content_encoding: None,
                content_language: None,
                content_length: None,
                content_md5: None,
                content_type: None,
                expires: None,
                grant_full_control: None,
                grant_read: None,
                grant_read_acp: None,
                grant_write_acp: None,
                metadata: None,
                request_payer: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                ssekms_key_id: None,
                server_side_encryption: None,
                storage_class: None,
                tagging: None,
                website_redirect_location: None,
                key: file_path.to_string()
            }
        ).sync();

        if response.is_err() {
            println!("Can't save the data to S3. - {}", format!("{:?}", response.unwrap_err()));
            return false;
        }

        return true;
    }
}
