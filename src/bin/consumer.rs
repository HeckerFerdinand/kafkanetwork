use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::Message;

#[tokio::main]
async fn main() {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "100.68.140.48:9092")
        .set("group.id", "test-gruppe")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("Consumer konnte nicht erstellt werden");

    consumer
        .subscribe(&["test"])
        .expect("Subscribe fehlgeschlagen");

    println!("warte auf Nachrichten ...");

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let text = msg
                    .payload()
                    .map(|p| String::from_utf8_lossy(p).into_owned())
                    .unwrap_or_default();
                println!(
                    "partition={} offset={} text={}",
                    msg.partition(),
                    msg.offset(),
                    text
                );
            }
            Err(err) => println!("Fehler: {err}"),
        }
    }
}
