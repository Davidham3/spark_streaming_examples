# spark-submit --jars /opt/cloudera/parcels/CDH-5.7.2-1.cdh5.7.2.p0.18/lib/spark/lib/spark-examples-1.6.0-cdh5.7.2-hadoop2.6.0-cdh5.7.2.jar

zk_host = 'tzb-04'
hbase_table = 'tzb:realSimulation'
keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
conf = {"hbase.zookeeper.quorum": zk_host,
        "hbase.mapred.outputtable": hbase_table,
        "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
 
rawData = ['3,A,name,Rongcheng','4,A,name,Guanhua']
// ( rowkey , [ row key , column family , column name , value ] )
sc.parallelize(rawData)\
    .map(lambda x: (x[0],x.split(',')))\
    .saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)