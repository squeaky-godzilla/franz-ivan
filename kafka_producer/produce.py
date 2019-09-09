'''
Kafka Producer for Franz-Ivan Project

Produce cryptocurrency prices messages for a Kafka topic. Data is relevant
for Coinbase-Pro and streamed from Cryptowatch API

Kafka topic is named same as the streamed crypto pair.

For valid crypto pairs visit: https://api.cryptowat.ch/pairs
'''

__author__ = "Vitek Urbanec"
__license__ = "MIT"
__maintainer__ = "Vitek Urbanec"


import time
import json
import argparse
from os import environ
import sys
import logging
import requests
from kafka import KafkaProducer
from kafka import errors as kafka_errors

MSG = '''
Kafka Producer for Franz-Ivan Project

Produce cryptocurrency prices messages for a Kafka topic. Data is relevant
for Coinbase-Pro and streamed from Cryptowatch API

Kafka topic is named same as the streamed crypto pair.

For valid crypto pairs visit: https://api.cryptowat.ch/pairs

'''




PARSER = argparse.ArgumentParser(description=MSG)

# parser.add_argument('--topic',action='store',dest='kafka_topic')
PARSER.add_argument('--host', action='store', dest='kafka_host')
PARSER.add_argument('--port', action='store', dest='kafka_port')
PARSER.add_argument('--crypto-pair', action='store', dest='crypto_pair', default="btcusd")
PARSER.add_argument('--envvars', action='store_true')

ARGS = PARSER.parse_args()

# logging setup


FORMATTER = logging.Formatter('%(asctime)-15s %(name)-12s: %(levelname)-8s %(message)s')

LOGGER = logging.getLogger()
HANDLER = logging.StreamHandler()
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)


if ARGS.envvars:

    try:
        KAFKA_TOPIC = environ['KAFKA_TOPIC']
        CRYPTO_PAIR = environ['CRYPTO_PAIR']
        KAFKA_HOST_PORT = environ['KAFKA_HOST_PORT']
        KAFKA_SSL_CAFILE = environ['KAFKA_SSL_CAFILE']
        KAFKA_SSL_CERTFILE = environ['KAFKA_SSL_CERTFILE']
        KAFKA_SSL_KEYFILE = environ['KAFKA_SSL_KEYFILE']
        LOGGING_LEVEL = environ['LOGGING_LEVEL']
    except KeyError:
        LOGGER.setLevel(logging.ERROR)
        LOGGER.error("Incomplete environment variables")
        sys.exit(1)

else:

    KAFKA_TOPIC = ARGS.crypto_pair
    KAFKA_HOST = ARGS.kafka_host
    KAFKA_PORT = str(ARGS.kafka_port)
    CRYPTO_PAIR = ARGS.crypto_pair


LOGGING_LEVEL_SETTING = logging.getLevelName(LOGGING_LEVEL)
LOGGER.setLevel(LOGGING_LEVEL_SETTING)


def get_crypto_price(crypto_pair_name):
    '''retrieves crypto price data from cryptowat.ch and checks for query error'''
    data = {}
    response = requests.get('https://api.cryptowat.ch/markets/coinbase-pro/%s/price'
                            % crypto_pair_name.lower())
    response_json = response.json()
    if "error" in response_json.keys():
        LOGGER.error("Invalid crypto pair")
        sys.exit(1)
    else:
        data[crypto_pair_name] = response_json
    return data



if __name__ == "__main__":

    # Wait for Kafka broker to become available
    
    for i in range(1, 10):
        try:
            LOGGER.error('connecting to Kafka ... \n')
            producer = \
                    KafkaProducer(
                        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                        bootstrap_servers=KAFKA_HOST_PORT,
                        security_protocol="SSL",
                        ssl_cafile=KAFKA_SSL_CAFILE,
                        ssl_certfile=KAFKA_SSL_CERTFILE,
                        ssl_keyfile=KAFKA_SSL_KEYFILE,
                        )
            LOGGER.info('Connected to Kafka\n')
        except Exception as e:
            LOGGER.info('Cannot connect to Kafka because %s, retrying in 10s', e)
            time.sleep(10)

    while True:
        DATA = get_crypto_price(CRYPTO_PAIR)
        producer.send(KAFKA_TOPIC, DATA)
        time.sleep(10)
