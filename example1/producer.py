# -*- coding:utf-8 -*-
from pykafka import KafkaClient
import time
import random

def readFile(filename):
    with open(filename, 'r') as f:
        for line in f:
            yield line.strip()

def send_records(topic_name, filename):
    '''send the records of the file to the specific topic

    Parameters
    ----------
    topic_name: str
        the name of the topic which you will sent records to

    filename: str
        the path of the file you want to read
    '''
    client = KafkaClient(hosts="172.31.42.38:9092,172.31.43.104:9092,172.31.43.105:9092")
    topic = client.topics[topic_name.encode('utf-8')]
    with topic.get_sync_producer() as producer:
        for i in readFile(filename):
            producer.produce(i.encode('utf-8'))
            print('produce %s'%(i))
            time.sleep(random.uniform(0, 0.5))

if __name__ == "__main__":
    send_records('userbehavior', 'E3_data/userBehavior/userBehavior')
