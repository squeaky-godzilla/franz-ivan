'''
Kafka Consumer for Franz-Ivan Project

Consume cryptocurrency prices messages from a Kafka topic.
Record the data into PostgreSQL DB, timestamp entered on DB record

For valid crypto pairs visit: https://api.cryptowat.ch/pairs
'''

__author__ = "Vitek Urbanec"
__license__ = "MIT"
__maintainer__ = "Vitek Urbanec"

import json
from os import environ
import time
import sys
import logging
from kafka import KafkaConsumer, TopicPartition

# Postgres imports

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from dotenv import load_dotenv

# loading env file

load_dotenv(verbose=True)

# logging setup

FORMATTER = logging.Formatter('%(asctime)-15s %(name)-12s: %(levelname)-8s %(message)s')

LOGGER = logging.getLogger()
HANDLER = logging.StreamHandler()
HANDLER.setFormatter(FORMATTER)
LOGGER.addHandler(HANDLER)


try:
    DB_HOST = environ['DB_HOST']
    DB_PORT = environ['DB_PORT']
    KAFKA_TOPIC = environ['KAFKA_TOPIC']
    CRYPTO_PAIR = environ['CRYPTO_PAIR']
    KAFKA_HOST = environ['KAFKA_HOST']
    KAFKA_PORT = environ['KAFKA_PORT']
    KAFKA_SSL_CAFILE = environ['KAFKA_SSL_CAFILE']
    KAFKA_SSL_CERTFILE = environ['KAFKA_SSL_CERTFILE']
    KAFKA_SSL_KEYFILE = environ['KAFKA_SSL_KEYFILE']
    DB_NAME = environ['DB_NAME']
    DB_USER = environ['DB_USER']
    DB_PASS = environ['DB_PASS']
    LOGGING_LEVEL = environ['LOGGING_LEVEL']
except KeyError:
    LOGGER.setLevel(logging.ERROR)
    LOGGER.error("Incomplete environment variables")
    sys.exit(1)

LOGGING_LEVEL_SETTING = logging.getLevelName(LOGGING_LEVEL)
LOGGER.setLevel(LOGGING_LEVEL_SETTING)



if __name__ == "__main__":

    # Wait for Kafka broker to become available

    for i in range(1, 10):
        try:
            LOGGER.info('connecting to Kafka broker\n')
            consumer = \
                KafkaConsumer(
                    auto_offset_reset='latest',
                    bootstrap_servers=':'.join([KAFKA_HOST, KAFKA_PORT]),
                    security_protocol="SSL",
                    ssl_cafile=KAFKA_SSL_CAFILE,
                    ssl_certfile=KAFKA_SSL_CERTFILE,
                    ssl_keyfile=KAFKA_SSL_KEYFILE,
                    )
            LOGGER.info('Connected to Kafka\n')
        except Exception as e:
            LOGGER.info('Cannot connect to Kafka because %s, retrying in 10s', e)
            time.sleep(10)
    try:
        consumer.assign([TopicPartition(KAFKA_TOPIC, 0)])
    except NameError:
        LOGGER.error('Kafka connection failed')
        sys.exit(1)


    # Set up PostgreSQL connection

    try:
        LOGGER.info("PostgreSQL connection created")
        DB_CONNECTION = connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            database=DB_NAME,
            port=DB_PORT
            )
        DB_CONNECTION.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        LOGGER.info("PostgreSQL connection created")
    except Exception as e:
        LOGGER.info('Cannot connect to PostgreSQL because %s, retrying in 10s', e)
        time.sleep(10)

    # set up cursor
    DB_CURSOR = DB_CONNECTION.cursor()

    DB_CURSOR.execute(
        'CREATE TABLE IF NOT EXISTS %s (timestamp timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, value float)' % KAFKA_TOPIC)
    DB_CURSOR.execute(
        'CREATE TABLE IF NOT EXISTS cryptowatch_api_allowance (timestamp timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, allowance bigint)')

    for message in consumer:

        json_string = message.value.decode('utf8')
        try:
            json_data = json.loads(json_string)

            DB_CURSOR.execute('INSERT INTO %s(value) VALUES (%s)' %
                              (KAFKA_TOPIC, str(json_data[CRYPTO_PAIR]['result']['price'])))

            DB_CURSOR.execute('INSERT INTO %s(allowance) VALUES (%s)' %
                              ("cryptowatch_api_allowance",
                               str(json_data[CRYPTO_PAIR]['allowance']['remaining'])))

            LOGGER.info("Kafka timestamp: %s Record: %s\n" % (str(message.timestamp), str(json_data)))
        except Exception as e:
            LOGGER.error("Cannot record to PostgreSQL because: %s", e)
