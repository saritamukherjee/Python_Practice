import os
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime, timedelta
from urllib.request import Request, urlopen
from io import BytesIO
import zipfile
from kafka import KafkaProducer,KafkaConsumer
from kafka import TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
import pandas as pd
import json
from json import dumps
from configparser import ConfigParser
import time
import sys
import logging
logger = logging.getLogger()

c=0

header = {
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4',
    'Host': 'www1.nseindia.com',
    'Referer': 'https://www1.nseindia.com/',
    'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'}
kafka_bootstrap_servers='10.102.80.24:9092'
topic='nse_data'
scrapeDataFromYear=2019


def on_send_error(excp):
    logger.error('I am an errback', exc_info=excp)

#Create kafka Topic
def createKafkaTopic(topic_name,PARTITIONS,REPLICATION_FACTOR,KAFKA_BROKERS):
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
    topic_list = [NewTopic(name=topic_name, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

#converting the data into json
def data(df):
    data=df.to_json(orient='index')
    data=json.loads(data)
    i=0
    while i<=len(data)-1:
        yield data[str(i)]
        i+=1

#Scrapes data from website and writes to kafka Topic
def scrape(link,year,producer,day,month,TOPIC_NAME,partitionId):
    startTime=time.time()
    try:
        req = Request(link, headers=header)
        request = urlopen(req, timeout=5)
        status_code = request.getcode()
        if status_code==200:
            zip_file = zipfile.ZipFile(BytesIO(request.read()))
            names = zip_file.namelist()
            for name in names:
                df=pd.read_csv(zip_file.open(name))
                try:
                    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
                except:
                    pass
                msg_size=0
                msg_count=df.shape[0]
                for record in data(df):
                    producer.send(TOPIC_NAME, value=record, partition=partitionId).add_errback(on_send_error)
                    msg_size+=int(sys.getsizeof(str(record).encode("utf-8")))
                producer.flush()
                endTime=time.time()
                timeDiff=endTime-startTime
                print(f"{year}-{month}-{day} data inserted to topic-{TOPIC_NAME} of partition-{partitionId}")
                print(f"INSERTED {msg_count} records in {timeDiff:.2f} seconds")
    except:
        logger.info(f"{year}-{month}-{day} is NSE holiday")

#consumer will subscribe the message from the topic
def consumer():
    try:
        KAFKA_BROKERS = kafka_bootstrap_servers
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BROKERS,
                                auto_offset_reset='earliest',
                                consumer_timeout_ms=1000)
        consumer.subscribe([topic])

        for message in consumer:
            print("Writing the message to consumer")
            print(message)
    except:
        logger.info('error')

    #consumer.close()

def main():
    DATA_FROM_YEAR=scrapeDataFromYear
    KAFKA_BROKERS=kafka_bootstrap_servers
    TOPIC_NAME=topic
    PARTITIONS=3
    REPLICATION_FACTOR=1
    lastdate=f"{DATA_FROM_YEAR}-01-01"
    now=datetime.now().strftime("%Y-%m-%d")
    start = datetime.strptime(str(lastdate), "%Y-%m-%d")
    end = datetime.strptime(now, "%Y-%m-%d")
    date_generated = [start + timedelta(days=x) for x in range(1, (end-start).days+1)]
    print("Producer starting")
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS,value_serializer=lambda x: dumps(x).encode('utf-8'))
        print("Producer started")
    except Exception as e:
        logger.exception(e)
        #raise e
    try:
        topicCreation=createKafkaTopic(TOPIC_NAME, PARTITIONS, REPLICATION_FACTOR, KAFKA_BROKERS)
        print(f'{TOPIC_NAME} is created')
    except Exception as e:
        logger.exception(e)
        #raise e

    partitionYearsandId = []
    tempPartitionId = 0
    for date in date_generated:
        day=date.strftime("%d")
        month=(date.strftime("%B")[:3]).upper()
        year=date.strftime("%Y")
        if not any(year in x for x in partitionYearsandId):
            partitionYearsandId.append({year:tempPartitionId})
            tempPartitionId+=1
        partitionId=[x.get(year) for x in partitionYearsandId if x.get(year)!=None][0]
        fname="cm"+str(day)+str(month)+str(year)+"bhav.csv.zip"
        link="https://www1.nseindia.com/content/historical/EQUITIES/"+str(year)+"/"+month+"/"+fname
        scrape(link,year,producer,day,month,TOPIC_NAME,partitionId)
        consumer()
if __name__ == '__main__':
    main()

