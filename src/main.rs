#![feature(plugin)]
#![feature(duration_as_u128)]
#![plugin(rocket_codegen)]
extern crate rocket;

#[macro_use]
extern crate hyper;

#[macro_use]
extern crate serde_derive;

extern crate rusoto_core;
extern crate rusoto_kinesis;
extern crate rusoto_dynamodb;
extern crate tokio_core;
extern crate threadpool;
extern crate uuid;
extern crate serde_json;
extern crate chrono;
extern crate rusoto_s3;
extern crate crypto;
extern crate rusoto_sts;
extern crate zip;
extern crate b64;
extern crate libflate;
extern crate env_logger;
extern crate rusoto_credential;

mod kinesis_stream;
mod dynamo_db;

use kinesis_stream::kcl::KinesisStreamLibrary;
use dynamo_db::dynamo_db_library::DynamoDbLibrary;

use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};
use std::io::Read;
use libflate::gzip::Decoder;
use uuid::Uuid;
use std::time;
use std::str;
use std::thread;
use std::time::{Duration, Instant};
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader, Write};
use hyper::*;
use hyper::header::HeaderValue;
use hyper::rt::{self, Future, Stream};
use hyper::client::HttpConnector;
use rusoto_core::Region;
use rusoto_core::ProvideAwsCredentials;
use rusoto_core::credential::{AwsCredentials, DefaultCredentialsProvider, ChainProvider};
use rusoto_kinesis::*;
use rusoto_dynamodb::*;
use rusoto_s3::*;
use threadpool::ThreadPool;
use std::collections::HashMap;
use tokio_core::reactor;
use serde_json::{Value, Error};
use rusoto_sts::{StsClient, StsAssumeRoleSessionCredentialsProvider};
use std::sync::Arc;
use std::env::args;

const IAM_ROLE_ARN: &str = "arn:aws:iam::123456:role/iam-role-1234";

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut is_debug_enabled = false;
    for argument in args {
        if argument.to_string() == "debug".to_string() {
            is_debug_enabled = true;
        }
    }

    let _ = env_logger::try_init();
    let region = Region::EuWest1;
    let dynamo_db_library = DynamoDbLibrary::new(STREAM_NAME_STR.to_string());
    let s3_client = S3Client::new(region);
    let kcl =
        Arc::new(
            KinesisStreamLibrary::new(
                IAM_ROLE_ARN.to_string(),
                dynamo_db_library,
                s3_client,
                is_debug_enabled
            ));

    /// Handle throttling exception with exponential back_off mechanism.
    let mut stream_shards_option = kcl.get_stream_shards();
    for i in 0..5 {
        if stream_shards_option.is_none() {
            println!("Describing stream exception.");
            let back_off_time = kcl.get_back_off_milli(i);
            let sleep_time = time::Duration::from_millis(back_off_time);
            thread::sleep(sleep_time);

            stream_shards_option = kcl.get_stream_shards();
        } else {
            break;
        }
    }

    let stream_shards = stream_shards_option.unwrap();
    let pool = ThreadPool::new(5);
    let worker_uuid = Uuid::new_v4();
    println!("Worker UUID: {}", worker_uuid);

    /// It's a separate thread for the AWS health-api.
    pool.execute(move || {
        rocket::ignite().mount("/", routes![health_api]).launch();
    });

    loop {
        let kinesis_stream_shards = stream_shards.clone();
        kcl.cut_the_loose_for_the_idle_shards(&kinesis_stream_shards);
        for stream_shard in kinesis_stream_shards {
            let kcl_arc_clone = kcl.clone();
            pool.execute(move || {
                let worker_unique_id = format!("{}-{:?}", worker_uuid, thread::current().id());

                /// Start processing the shard records.
                kcl_arc_clone.read_from_given_shard(
                    &worker_unique_id, &(stream_shard.shard_id)
                );
            });
        }
    }
}

#[get("/")]
fn health_api() -> String {
    let json_string = r#"{"status":"UP"}"#;
    let response: Value = serde_json::from_str(json_string).unwrap();

    return serde_json::to_string(&response).unwrap().to_string();
}
