# -*- coding:utf-8 -*-

#
# spark-submit --master yarn --num-executors 32 --executor-memory 2G --executor-cores 4 --jars jars/spark-examples_2.10_my_converters-1.6.0.jar spark_streaming.py
#
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import redis

redis_host = 'tzb-06'
redis_port = 6379
redis_db = 1
r = redis.Redis(connection_pool=redis.ConnectionPool(host=redis_host, port=redis_port, db=redis_db))

conf = SparkConf().set("spark.executorEnv.PYTHONHASHSEED", "0").setAppName("kafka_hbase")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 5)

broker = 'tzb-05:9092,tzb-06:9092,tzb-07:9092'
topics = ["csv_pc3_800h_KT_20190101_DJD_csv", 'csvTopic']
zk_host = 'tzb-02'

conf = {"hbase.zookeeper.quorum": zk_host,
        # "hbase.mapred.outputtable": hbase_table_name,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToBigDecimalToPutConverter"

def transform(hbase_value_dict):
    returns = []
    filename = hbase_value_dict['filename']
    tablename = hbase_value_dict['tablename']
    if len(hbase_value_dict['data']) != 1:
        return []
    for rowkey, col_family in hbase_value_dict['data'].items():
        for col, value in col_family.items():
            f, q = col.split(':')
            returns.append((rowkey, [rowkey, f, q, value]))
    return (filename, [rowkey]), (tablename, returns)

def save_to_hbase(rdd, conf, keyConv, valueConv):
    '''
    rdd 内的每个元素都是(tablename, returns)
    '''
    # 获取所有的表名
    tablenames = rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y).map(lambda x: x[0]).collect()
    print(tablenames)
    for table in tablenames:
        conf["hbase.mapred.outputtable"] = table
        t = rdd.filter(lambda x: True if x[0] == table else False).flatMap(lambda x: x[1])
        print(t.count())
        t.saveAsNewAPIHadoopDataset(
                conf=conf,
                keyConverter=keyConv,
                valueConverter=valueConv
            )

def update_redis(rdd, r):
    for filename, current_rowkeys in rdd.collect():
        target_rowkeys_values = r.get(filename)
        if not target_rowkeys_values:
            return
        target_rowkeys = set(target_rowkeys_values.decode().split(';'))
        rest_rowkeys = target_rowkeys - current_rowkeys
        if not rest_rowkeys:
            r.delete(filename)
        else:
            r.set(filename, ';'.join(rest_rowkeys))

kvs = KafkaUtils.createDirectStream(ssc, topics, kafkaParams={"metadata.broker.list": broker})
lines = kvs.map(lambda x: x[1])\
            .map(json.loads)\
            .map(transform)\
            .filter(lambda x: True if x else False)\
            .cache()

lines.map(lambda x: x[1])\
    .foreachRDD(lambda x: save_to_hbase(x, conf, keyConv, valueConv))

lines = lines.map(lambda x: x[0])\
            .reduceByKey(lambda x, y: x + y)\
            .mapValues(lambda x: set(x)).cache()

lines.foreachRDD(lambda rdd: update_redis(rdd, r))
lines.map(lambda x: (x[0], len(x[1]))).pprint()

ssc.start()
ssc.awaitTermination()