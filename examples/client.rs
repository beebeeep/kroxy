use std::collections::HashMap;

use clap::{Parser, command};
use kroxy::grpc;
use tokio::{
    io::{self, AsyncBufReadExt},
    sync::mpsc,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::transport::Channel;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "http://localhost:50051")]
    proxy: String,
    #[arg(short, long)]
    topic: String,
    #[arg(short, long)]
    consumer: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let client = grpc::kafka_proxy_client::KafkaProxyClient::connect(
        Channel::from_shared(args.proxy).unwrap(),
    )
    .await
    .expect("connecting to proxy");
    if args.consumer {
        consumer(&args.topic, client).await;
    } else {
        producer(&args.topic, client).await;
    }
}

async fn producer(topic: &str, mut client: grpc::kafka_proxy_client::KafkaProxyClient<Channel>) {
    let stdin = io::stdin();
    let reader = io::BufReader::new(stdin);
    let mut input = reader.lines();
    println!("Reading messages from stdin, one per line");
    while let Some(line) = input.next_line().await.unwrap() {
        let req = tonic::Request::new(grpc::ProduceRequest {
            topic: String::from(topic),
            brokers: Vec::new(),
            durability: grpc::Durability::All.into(),
            messages: vec![grpc::Message {
                key: Vec::new(),
                value: line.into(),
                timestamp: None,
                headers: HashMap::new(),
            }],
        });
        client.produce(req).await.unwrap();
    }
}

async fn consumer(topic: &str, mut client: grpc::kafka_proxy_client::KafkaProxyClient<Channel>) {
    loop {
        let (req_tx, req_rx) = mpsc::channel(1);
        let (msg_tx, mut msg_rx) = mpsc::channel(1);
        let topic = String::from(topic);

        tokio::spawn(async move {
            // message handler that receive message from proxy and sends the next consume request

            loop {
                req_tx
                    .send(grpc::ConsumeRequest {
                        topic: topic.clone(),
                        brokers: Vec::new(),
                        consumer_group: String::from("kroxy-example"),
                        ack_previous_message: true,
                    })
                    .await
                    .unwrap();

                let msg = match msg_rx.recv().await {
                    None => {
                        return;
                    }
                    Some(x) => x,
                };
                println!("got message: {}", String::from_utf8(msg).unwrap());
            }
        });

        let req_stream = ReceiverStream::new(req_rx);
        let mut resp_stream = client.consume(req_stream).await.unwrap().into_inner();
        while let Some(resp) = resp_stream.next().await {
            match resp.unwrap().message {
                Some(msg) => {
                    let _ = msg_tx.send(msg.value).await.unwrap();
                }
                _ => {
                    return;
                }
            }
        }
    }
}
