import requests
import datetime
import json
from confluent_kafka import Producer

class DataSource:
    def __init__(self):
        pass



    @staticmethod
    def start():
        url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        topic = "btc-price"
        producer_config = {'bootstrap.servers' : 'localhost:9092'}

        producer = Producer(producer_config)
        while True:
            res = requests.get(url)
            received_time = datetime.datetime.now(datetime.timezone.utc)
            if res.status_code == 200:
                data = res.json()
                data['timestamp'] = received_time
                print(data)

            break


DataSource.start()