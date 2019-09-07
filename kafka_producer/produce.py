from kafka import KafkaProducer
from kafka import errors as kafka_errors
import time
import json
import argparse
import requests
from os import environ
import sys
import logging

MSG = '''

Produce cryptocurrency prices messages for a Kafka topic. Data is relevant
for Coinbase-Pro and streamed from Cryptowatch API

Kafka topic is named same as the streamed crypto pair.

For valid crypto pairs visit: https://api.cryptowat.ch/pairs

'''

# logging setup

formatter = logging.Formatter('%(asctime)-15s %(name)-12s: %(levelname)-8s %(message)s')

logger = logging.getLogger()
handler = logging.StreamHandler()   # by default writes to STDERR when stream is None
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)

parser = argparse.ArgumentParser(description=MSG)

# parser.add_argument('--topic',action='store',dest='kafka_topic')
parser.add_argument('--host',action='store',dest='kafka_host')
parser.add_argument('--port',action='store',dest='kafka_port')
parser.add_argument('--crypto-pair',action='store',dest='crypto_pair',default="btcusd")
parser.add_argument('--envvars', action='store_true')

args = parser.parse_args()

if args.envvars:
    KAFKA_TOPIC = environ['KAFKA_TOPIC']
    KAFKA_HOST = environ['KAFKA_HOST']
    KAFKA_PORT = environ['KAFKA_PORT']
    CRYPTO_PAIR = environ['CRYPTO_PAIR']
    
else:

    KAFKA_TOPIC = args.crypto_pair
    KAFKA_HOST = args.kafka_host
    KAFKA_PORT = str(args.kafka_port)
    CRYPTO_PAIR = args.crypto_pair


# Wait for Kafka broker to become available
for i in range(1,10):
    try:
        logger.error('connecting to Kafka broker... \n')
        producer = KafkaProducer(value_serializer=lambda v:json.dumps(v).encode('utf-8'), 
                            bootstrap_servers=":".join([KAFKA_HOST, KAFKA_PORT]))
    except kafka_errors.NoBrokersAvailable:
        logger.error('No Kafka broker available, retrying in 10s... \n')
        time.sleep(10)

while True:
    response = requests.get('https://api.cryptowat.ch/markets/coinbase-pro/%s/price' % CRYPTO_PAIR.lower())
    data = response.json()
    producer.send(KAFKA_TOPIC, data)
    time.sleep(10)