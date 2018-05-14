import boto3
import botocore
import threading, logging, time
from kafka import KafkaProducer
import smart_open

class Producer(threading.Thread):
        daemon = True

        def run(self):
            producer = KafkaProducer(bootstrap_servers=["18.236.5.16:9092","54.70.30.239:9092","54.70.225.14:9092"]) # connect to kafka servers
            bucket_name="insightprojecttest2" # define the bucket name on s3
            bucket = self.read_s3(bucket_name)
            for json_obj in bucket.objects.all():
                    json_file = "s3://{0}/{1}".format(bucket_name, json_obj.key)
                    for line in smart_open.smart_open(json_file):
                           producer.send("EC", line) # define topic name
                           
                           print(line)

        def read_s3(self,bucket_name):
                s3 = boto3.resource('s3')

                try:
                        s3.meta.client.head_bucket(Bucket=bucket_name)
                except botocore.exceptions.ClientError as e:
                        return None
                else:
                        return s3.Bucket(bucket_name)

def main():
        producer = Producer()
        producer.daemon = True
        producer.start()
        while True:
                time.sleep(0.000002) # read from producer every __ seconds

if __name__ == "__main__":
        main()