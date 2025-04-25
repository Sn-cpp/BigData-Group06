import requests
import datetime
import json
import time
from confluent_kafka import Producer

class DataSource:
    def __init__(self):
        pass

    @staticmethod
    def start():
        """Main method to start crawling"""

        #API link
        url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        
        #Kafka Producer and topic name
        topic = "btc-price"
        producer_config = {'bootstrap.servers' : 'localhost:9092'}

        producer = Producer(producer_config)

        #Loop for crawling and publishing
        while True:
            #Crawl data
            res = requests.get(url)
            
            #Get timestamp
            received_time = datetime.datetime.now(datetime.timezone.utc)
            
            #Only publish to topic if received a successful response
            if res.status_code == 200:
                #Parse the response as a JSON object
                data = res.json()

                #Add timestamp field
                data['timestamp'] = received_time

                #print(data)
                
                #Publish to the topic
                producer.produce(topic, value=json.dumps(data))
                producer.poll(0)

            break
            time.sleep(0.1)

DataSource.start()