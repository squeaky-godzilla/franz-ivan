from kafka import KafkaProducer
import time
import json
import argparse
import requests

MSG = '''

Produce cryptocurrency prices messages for a Kafka topic. Data is relevant
for Coinbase-Pro and streamed from Cryptowatch API

Kafka topic is named same as the streamed crypto pair.

For valid crypto pairs visit: https://api.cryptowat.ch/pairs

'''

parser = argparse.ArgumentParser(description=MSG)

# parser.add_argument('--topic',action='store',dest='kafka_topic')
parser.add_argument('--host',action='store',dest='kafka_host',required=True)
parser.add_argument('--port',action='store',dest='kafka_port',required=True)
parser.add_argument('--crypto-pair',action='store',dest='crypto_pair',required=True, default="btcusd")

args = parser.parse_args()

KAFKA_TOPIC = args.crypto_pair
KAFKA_HOST = args.kafka_host
KAFKA_PORT = str(args.kafka_port)
CRYPTO_PAIR = args.crypto_pair

producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'), bootstrap_servers=":".join([KAFKA_HOST, KAFKA_PORT]))

while True:
    response = requests.get('https://api.cryptowat.ch/markets/coinbase-pro/%s/price' % CRYPTO_PAIR.lower())
    data = response.json()
    producer.send(KAFKA_TOPIC, data)
    time.sleep(10)