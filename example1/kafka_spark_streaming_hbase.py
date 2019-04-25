# -*- coding:utf-8 -*-
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import happybase
import json
import time
 
conf = SparkConf().setAppName("kafka_hbase")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 60)

broker = 'cluster2:9092,cluster3:9092,cluster4:9092'
topic = "userbehavior"
hbaseZK = "cluster2:9092"
hbase_ip = 'cluster3'
table = 'testUserBehavior'

def fmt_data(line):
    uid, behavior, aid, time = line.strip().split('\x01')

pool = happybase.ConnectionPool(size = 3, host = hbase_ip)
count = 0  # rowkey

def write_into_hbase(msg):
    global count

    with pool.connection() as connection:
        connection.open()

    hbaseTable = connection.table(table.encode('utf-8'))

    b = hbaseTable.batch()
    if not msg.isEmpty():
        msg_rdd = msg.map(lambda x: x[1].split('\x01'))
        msg_list = msg_rdd.collect()
        for data in msg_list:
            if len(data) != 4:
                continue
            uid, behavior, aid, timestamp = data
            data_dict = {b'INFO:Uid': uid.encode('utf-8'),
                         b'INFO:Aid': aid.encode('utf-8'),
                         b'INFO:behavior': behavior.encode('utf-8'),
                         b'INFO:Time': timestamp.encode('utf-8')}
            b.put(row = str(count).encode('utf-8'), data = data_dict)
            b.send()
            count += 1
            
    connection.close()
 
kafkaStreams = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list": broker})
kafkaStreams.foreachRDD(write_into_hbase)

ssc.start()
ssc.awaitTermination()