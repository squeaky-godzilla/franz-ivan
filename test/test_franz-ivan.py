import pytest
import subprocess
from dotenv import load_dotenv





'''
Producer tests
==============

    - missing envvars
    - non-existent crypto pair

Consumer tests
==============

    - missing envvars
    - cannot connect to kafka address:port
    - cannot connect to postgres address:port

Integration tests
=================

    - test table not recording
    - test topic in kafka not getting messages

'''
    
def test_consumer_missing_envvars():
    load_dotenv("./test_cases/missing_envvars/.env")
    child = subprocess.Popen(['python3', '../kafka_consumer/consume.py'])
    child.communicate()[0]
    exitcode = child.returncode
    assert exitcode == 1

def test_producer_missing_envvars():
    load_dotenv("./test_cases/missing_envvars/.env")
    child = subprocess.Popen(['python3', '../kafka_producer/produce.py'])
    child.communicate()[0]
    exitcode = child.returncode
    assert exitcode == 1
  