# -*- coding:utf-8 -*-

# create kafka topic 
# kafka-topics --create --zookeeper tzb-02:2181 --replication-factor 3 --partitions 60 --topic csv_pc3_800h_KT_20190101_DJD_csv

from datetime import datetime, timedelta
import random
import json
import redis
import os
import queue
import time
import concurrent.futures

from pykafka import KafkaClient

# 新建redis连接
r = redis.Redis(connection_pool=redis.ConnectionPool(host='tzb-06', port=6379, db=1))

# 震动数据的列
cols = ['0x00010001',
        '0x00010002',
        '0x00010003',
        '0x00010004',
        '0x00010005',
        '0x00010006',
        '0x00010007',
        '0x00010008',
        '0x00010009',
        '0x00010010',
        '0x00010011',
        '0x00010012',
        '0x00010013',
        '0x00010014',
        '0x00010015',
        '0x00010016']

def generate_data():
    '''
    生成数据
    '''
    if not os.path.exists('data'):
        os.mkdir('data')
    current_time = datetime.strptime('2019-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    delta = timedelta(seconds=1)
    for day in range(7):
        with open('data/csv_%s.txt'%(day), 'w') as f:
            for i in range(3600 * 24):
                rowkey = current_time.strftime('%S%Y-%m-%d %H:%M:%S')
                value = json.dumps({
                    'data': {rowkey: {'INFO:%s'%(col): str(random.random()) for col in cols}}
                })
                f.write(value)
                f.write('\n')
                current_time += delta
                print(rowkey)

def read_file(filename):
    '''
    读取文件
    '''
    with open(filename, 'r') as f:
        data = f.readline().strip()
        while data:
            yield data
            data = f.readline().strip()

def send_records(filename):
    '''
    发送数据
    参数：文件路径
    '''
    start_time = time.time()

    # 新建一个失败任务的队列，到时候重新发送
    q = queue.Queue()

    # 指定发送到的topic，后期的时候根据filename，计算出发送到的topic
    # 并且在这里增加创建topic的命令
    topic_name = 'csv_pc3_800h_KT_20190101_DJD_csv'

    # 创建kafka客户端
    client = KafkaClient(hosts="tzb-05:9092,tzb-06:9092,tzb-07:9092")

    # 选择要发送的topic
    topic = client.topics[topic_name.encode('utf-8')]

    # 统计文件内出现的所有的rowkey
    rowkeys = set()
    for line in read_file(filename):
        value = json.loads(line)['data']
        rowkeys.add(list(value.keys())[0])

    # 将文件路径作为键，rowkey用分号拼接的字符串作为值存入redis中
    r.set(filename, ';'.join(rowkeys))

    # 打印一下上传到redis中文件的rowkey有多少个
    print(filename, len(rowkeys))

    # 创建producer
    with topic.get_sync_producer() as producer:

        # 按行读文件
        for line in read_file(filename):

            # 每读一行解析一下
            value = json.loads(line)

            # 增加新的键叫 "filename"
            value['filename'] = filename

            value['tablename'] = 'csv:pc3_800h_KT_20190101_DJD_csv'

            try:
                # 将数据转换为json字符串，转为 bytes 后发送
                # pykafka 要求发送的数据是 bytes，java 不需要
                producer.produce(json.dumps(value).encode('utf-8'))
            except Exception as e:
                print(e)
                # 发送失败的话，把数据存入到队列中
                q.put(json.dumps(value).encode('utf-8'))

        # 将队列中的元素重新发送
        retry(producer, filename, q)
    
    # 打印发送完的文件
    # print(filename, 'finished!')
    return '%s finished! time: %.2fs'%(filename, time.time() - start_time)

def retry(producer, filename, q):
    # 统计重发的消息的数量
    count = 0

    # 队列不为空就取元素出来
    while not q.empty():
        try:
            # 取元素
            element = q.get()
            count += 1

            # 发送
            producer.produce(element)
        except Exception as e:
            print(e)
            print(element)
    if count != 0:
        print('%s retried number of elements: %s'%(filename, count))

if __name__ == "__main__":
    # generate_data()

    # 创建进程池
    with concurrent.futures.ProcessPoolExecutor(max_workers=10) as executor:
        # 将每个要发送的文件放到一个进程中发送，进程使用的函数是 send_records，参数就是文件名
        # 返回值存入一个字典中
        future_to_filename = {executor.submit(send_records, 'data/%s'%(filename)): filename for filename in os.listdir('data')}
        
        # 所有进程运行后，打印进程的返回结果
        for future in concurrent.futures.as_completed(future_to_filename):
            try:
                # 获取进程的运行结果
                data = future.result()
            except Exception as exc:
                # 如果进程运行的有问题，就打印错误
                filename = future_to_filename[future]
                print('%r generated an exception: %s' % (filename, exc))
            else:
                # 没问题就打印进程的返回结果
                print('%s' % (data))
