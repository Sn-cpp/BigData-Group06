import requests
import datetime
import json
import time
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        #print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        pass

class DataSource:
    def __init__(
        self,
        symbol: str = "BTCUSDT",
        kafka_servers: str = "localhost:9092",
        topic: str = "btc-price",
        poll_timeout: float = 0.0,
    ):
        self.symbol = symbol
        self.url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": kafka_servers})
        self.poll_timeout = poll_timeout
        self._running = False


    def start(self, interval: float = 0.1) -> None:
        self._running = True

        while self._running:
            try:
                response = requests.get(self.url)
                now = datetime.datetime.now(datetime.timezone.utc)

                if response.status_code == 200:
                    data = response.json()
                    data["timestamp"] = now.isoformat(timespec="seconds")
                    self.producer.produce(
                        self.topic, 
                        value=json.dumps(data), 
                        callback=delivery_report
                    )
                    print(f"Produced message: {data}")
                    self.producer.poll(self.poll_timeout)
                    
            except Exception as e:
                print(f"Error fetching data: {e}")

            time.sleep(interval)

    def flush(self, timeout: float = 10.0) -> None:
        self.producer.flush(timeout)

    def stop(self) -> None:
        self._running = False
        self.flush()

if __name__ == "__main__":
    source = DataSource()

    try:
        source.start()
    except KeyboardInterrupt:
        print("Stopping data source...")
        source.stop()
        print("Data source stopped.")
        