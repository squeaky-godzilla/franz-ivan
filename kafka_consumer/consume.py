from kafka import KafkaConsumer, TopicPartition, SimpleConsumer, SimpleClient
from kafka import errors as kafka_errors
import json
import argparse
from os import environ
import time
import sys
import logging
from datetime import datetime

# Postgres imports

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import OperationalError
from psycopg2 import errors



parser = argparse.ArgumentParser()

parser.add_argument('--topic',action='store',dest='kafka_topic')
parser.add_argument('--host',action='store',dest='kafka_host')
parser.add_argument('--port',action='store',dest='kafka_port')


parser.add_argument('--db-host',action='store',dest='db_host')
parser.add_argument('--envvars', action='store_true')

args = parser.parse_args()

if args.envvars:
    DB_HOST = environ['DB_HOST']
    DB_PORT = environ['DB_PORT']
    KAFKA_TOPIC = environ['KAFKA_TOPIC']
    CRYPTO_PAIR = environ['CRYPTO_PAIR']
    KAFKA_HOST_PORT = environ['KAFKA_HOST_PORT']
    KAFKA_SSL_CAFILE = environ['KAFKA_SSL_CAFILE']
    KAFKA_SSL_CERTFILE = environ['KAFKA_SSL_CERTFILE']
    KAFKA_SSL_KEYFILE = environ['KAFKA_SSL_KEYFILE']
    DB_NAME = environ['DB_NAME']
    DB_USER = environ['DB_USER']
    DB_PASS = environ['DB_PASS']
    LOGGING_LEVEL = environ['LOGGING_LEVEL']
    
else:

    KAFKA_TOPIC = args.crypto_pair
    KAFKA_HOST = args.kafka_host
    KAFKA_PORT = str(args.kafka_port)
    CRYPTO_PAIR = args.crypto_pair

# logging setup

formatter = logging.Formatter('%(asctime)-15s %(name)-12s: %(levelname)-8s %(message)s')

logger = logging.getLogger()
handler = logging.StreamHandler()   # by default writes to STDERR when stream is None
handler.setFormatter(formatter)
logger.addHandler(handler)
logging_level = logging.getLevelName(LOGGING_LEVEL)
logger.setLevel(logging_level)

# Wait for Kafka broker to become available
for i in range(1,10):
    try:
        logger.info('connecting to Kafka broker\n')
        consumer = \
            KafkaConsumer(      
                                auto_offset_reset='latest', 
                                bootstrap_servers=KAFKA_HOST_PORT,
                                security_protocol="SSL",
                                ssl_cafile=KAFKA_SSL_CAFILE,
                                ssl_certfile=KAFKA_SSL_CERTFILE,
                                ssl_keyfile=KAFKA_SSL_KEYFILE,
                            )
    except kafka_errors.NoBrokersAvailable:
        logger.info('No Kafka broker available, retrying in 10s\n')
        time.sleep(10)

consumer.assign([TopicPartition(KAFKA_TOPIC, 0)])

db_connection = connect(
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST, 
                            database=DB_NAME,
                            port=DB_PORT
                        )
db_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
logger.info("PostgreSQL connection created")


# set up cursor
db_cursor = db_connection.cursor()

db_cursor.execute('CREATE TABLE IF NOT EXISTS %s (timestamp timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, value float)' % KAFKA_TOPIC)
db_cursor.execute('CREATE TABLE IF NOT EXISTS cryptowatch_api_allowance (timestamp timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, allowance bigint)')

time.sleep(10)

for message in consumer:
    
    json_string = message.value.decode('utf8')
    try:
        # timestamp = datetime.fromtimestamp(message.timestamp)
        json_data = json.loads(json_string)
        
        
        db_cursor.execute('INSERT INTO %s(value) VALUES (%s)' % 
            (KAFKA_TOPIC, str(json_data[CRYPTO_PAIR]['result']['price'])))
        

        db_cursor.execute('INSERT INTO %s(allowance) VALUES (%s)' % 
            ("cryptowatch_api_allowance", str(json_data[CRYPTO_PAIR]['allowance']['remaining'])))

        logger.info("Kafka timestamp: %s Record: %s\n" % (str(message.timestamp), str(json_data)))
    except Exception as e:
        logger.error(e)