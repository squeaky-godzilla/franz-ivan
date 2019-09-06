from kafka import KafkaConsumer, TopicPartition, SimpleConsumer, SimpleClient
from kafka import errors as kafka_errors
import json
import argparse
from os import environ
import time
import sys
import logging

# Postgres imports

from psycopg2 import connect
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import OperationalError
from psycopg2 import errors

# logging setup

formatter = logging.Formatter('%(asctime)-15s %(name)-12s: %(levelname)-8s %(message)s')

logger = logging.getLogger()
handler = logging.StreamHandler()   # by default writes to STDERR when stream is None
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)


parser = argparse.ArgumentParser()

parser.add_argument('--topic',action='store',dest='kafka_topic')
parser.add_argument('--host',action='store',dest='kafka_host')
parser.add_argument('--port',action='store',dest='kafka_port')


parser.add_argument('--db-host',action='store',dest='db_host')
parser.add_argument('--envvars', action='store_true')

args = parser.parse_args()

if args.envvars:
    KAFKA_TOPIC = environ['KAFKA_TOPIC']
    KAFKA_HOST = environ['KAFKA_HOST']
    KAFKA_PORT = environ['KAFKA_PORT']
    CRYPTO_PAIR = environ['CRYPTO_PAIR']
    DB_HOST = environ['DB_HOST']
    
else:

    KAFKA_TOPIC = args.crypto_pair
    KAFKA_HOST = args.kafka_host
    KAFKA_PORT = str(args.kafka_port)
    CRYPTO_PAIR = args.crypto_pair

DB_NAME = "crypto_prices"

# Wait for Kafka broker to become available
for i in range(1,10):
    try:
        logger.error('connecting to Kafka broker\n')
        consumer = KafkaConsumer(bootstrap_servers=":".join([KAFKA_HOST, KAFKA_PORT]), auto_offset_reset='latest')
    except kafka_errors.NoBrokersAvailable:
        logger.error('No Kafka broker available, retrying in 10s\n')
        time.sleep(10)

consumer.assign([TopicPartition(KAFKA_TOPIC, 0)])

# set up Postgres connection and prices database
def init_connect_dbase():
    try:
        db_connection = connect(user='postgres',password='example',host=DB_HOST, database=DB_NAME)
        db_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except OperationalError:
        logger.error("first time record, creating DB...")
        db_connection = connect(user='postgres',password='example',host=DB_HOST)
        logger.error("connected to DB server...")
        db_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        logger.error("set isolation level")
        db_cursor = db_connection.cursor()
        logger.error("create cursor")
        db_cursor.execute("CREATE DATABASE %s" % DB_NAME)
        logger.error("create database %s" % DB_NAME)
        # close cursor and connection and reconnect
        db_cursor.close()
        logger.error("close cursor")
        db_connection.close()
        logger.error("close connection")
        logger.error("attempt connection to newly created DB")
        db_connection = connect(user='postgres',password='example',host=DB_HOST, database=DB_NAME)
        logger.error("connection successful!")
    return db_connection

db_connection = init_connect_dbase()
logger.error("connection created")


# set up cursor
db_cursor = db_connection.cursor()
logger.error("cursor set up")

# create table to record the time series
logger.error("creating the table %s" % KAFKA_TOPIC)
try:
    db_cursor.execute('CREATE TABLE %s (timestamp bigint PRIMARY KEY, value float)' % KAFKA_TOPIC)
except errors.DuplicateTable:
    logger.error('recording to existing table: %s' % KAFKA_TOPIC)

# create table to record Cryptowat.ch API call allowance
logger.error("creating the table cryptowatch_api_allowance")
try:
    db_cursor.execute('CREATE TABLE cryptowatch_api_allowance (timestamp bigint PRIMARY KEY, allowance bigint)')
except errors.DuplicateTable:
    pass

for message in consumer:
    logger.error("recording data....")

    json_string = message.value.decode('utf8')
    
    try:
        json_data = json.loads(json_string)
        
        # record time series to Postgres
        db_cursor.execute('INSERT INTO %s(timestamp, value) VALUES (%s, %s)' % 
            (KAFKA_TOPIC, str(message.timestamp), str(json_data['result']['price'])))
        
        # record API call allowance to Postgres
        db_cursor.execute('INSERT INTO %s(timestamp, allowance) VALUES (%s, %s)' % 
            ("cryptowatch_api_allowance", str(message.timestamp), str(json_data['allowance']['remaining'])))
        logger.error(json_data + '\n')
    except Exception as e:
        logger.error(e)