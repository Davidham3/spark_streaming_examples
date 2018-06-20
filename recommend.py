# -*- coding:utf-8 -*-
import json
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS
from pyspark.sql.types import *
from pyspark.sql import SQLContext

def change_format(line):
    t = {i['qualifier']: i['value'] for i in map(json.loads, line[1].split('\n'))}
    if 'Aid' not in t or 'Uid' not in t or 'behavior' not in t:
        return None
    else:
        try:
            uid = t['Uid'].replace('U', '')
            aid = t['Aid'].replace('D', '')
            return tuple(map(int, (uid, aid, int(t['behavior']))))
        except:
            return None

def output_format(line):
    try:
        user = str(line[0]).zfill(7)
        r = ', '.join('D' + str(i.product).zfill(7) for i in line[1])
        return ('U' + user, r)
    except:
        return None

def main(sc):
    hbaseconf = {"hbase.zookeeper.quorum": 'cluster3', "hbase.mapreduce.inputtable": 'testUserBehavior'}
    keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
    valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
    hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter = keyConv, valueConverter = valueConv, conf = hbaseconf)

    data = hbase_rdd.map(change_format).filter(lambda x: x is not None).cache()

    users = set(data.map(lambda x: (x[0], 1)) \
                .reduceByKey(lambda x, y: x + y) \
                .map(lambda x: (x[1], x[0])) \
                .sortByKey(ascending = False) \
                .map(lambda x: x[1]).take(1000))

    data = data.filter(lambda x: True if x[0] in users else False)

    model = ALS.trainImplicit(data, 1, seed = 10)
    results = model.recommendProductsForUsers(3).map(output_format).filter(lambda x: x is not None)

    sqlContext = SQLContext(sc)
    schema = StructType([
        StructField("Uid",StringType(),True),
        StructField("results",StringType(),True)
    ])
    r = sqlContext.createDataFrame(results, schema)

    url = "jdbc:mysql://cluster2"
    table = "CSDN.recommendation"
    properties = {"user": "root", "password": "cluster"}
    r.write.jdbc(url, table, 'overwrite', properties)

if __name__ == '__main__':
    conf = SparkConf().setAppName("recommend")
    sc = SparkContext(conf = conf)
    main(sc)