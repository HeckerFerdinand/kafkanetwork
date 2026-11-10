use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "100.68.140.48:9092")
        .create()
        .expect("Producer konnte nicht erstellt werden");
    
    for _ in 0..5{
        let record = FutureRecord::<str, str>::to("test").payload("message nr {:?} from consumer");
        match producer.send(record, Duration::from_secs(5)).await {
        Ok(delivery) => println!("gesendet: {delivery:?}"),
        Err((err, _msg)) => println!("Fehler: {err}"),
    }
    }
    
}