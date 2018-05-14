from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import json, math, datetime
import csv
import io
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
import json, math, datetime
from kafka.consumer import SimpleConsumer
from operator import add
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

# define functions to save rdds to cassandra
def sendCassandra(iter):
    print("send to cassandra")
    cluster = Cluster(['35.164.226.10', '52.24.193.4', '52.24.193.4', '54.245.7.62']) # connect to cassandra
    session = cluster.connect()
    session.execute('USE ' + "Energy_consumption") # provide keyspace
    insert_statement = session.prepare("INSERT INTO rawdata (Time, ID, Business, Scale, State, Ecoll, Efacility, Efan, Gfacility, Eheat, Gheat, Einterequip, Ginterequip, Einterlight, Gwater) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    count = 0

    # batch insert into cassandra database
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for record in iter:

        batch.add(insert_statement,(record[1][13], record[0], record[1][0], record[1][1], record[1][2], record[1][3], record[1][4],record[1][5], record[1][6], record[1][8], record[1][9], record[1][10], record[1][11], record[1][12]))


    #plit the batch, so that the batch will not exceed the size limit
        count += 1
        if count % 200== 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    print "saved"
    session.execute(batch)
    session.shutdown()


def sendCassandrastate(iter):
    print("send to cassandra")
    cluster = Cluster(['35.164.226.10', '52.24.193.4', '52.24.193.4', '54.245.7.62'])
    session = cluster.connect()
    session.execute('USE ' + "Energy_consumption")
    insert_statement = session.prepare("INSERT INTO statedata (Time, State, Ecoll, Efacility, Efan, Gfacility, Eheat, Gheat, Einterequip, Ginterequip, Einterlight, Gwater, Etotal, Gtotal) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?)")
    count = 0

    # batch insert into cassandra database
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for record in iter:

        # create new columns when writing data to cassandra, it saves much more time than doing a map job in the streaming process then save to cassandra
        num11 =record[1][3]+record[1][4]+record[1][5]+record[1][7]+record[1][9]+record[1][11]
        num12 =record[1][6]+record[1][8]+record[1][10]+record[1][12]+record[1][11]
        batch.add(insert_statement,(record[1][13], record[1][2], record[1][3], record[1][4], record[1][5], record[1][6], record[1][7], record[1][8], record[1][9], record[1][10], record[1][11], record[1][12], num11, num12))

        #split the batch, so that the batch will not exceed the size limit
        count += 1
        if count % 200== 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    print "saved"

# send the batch that is less than 500            
    session.execute(batch)
    session.shutdown()

def sendCassandrarecent(iter):

    print("send to cassandra")
    cluster = Cluster(['35.164.226.10', '52.24.193.4', '52.24.193.4', '54.245.7.62'])
    session = cluster.connect()
    session.execute('USE ' + "Energy_consumption")
    insert_statement = session.prepare("INSERT INTO recentdata (ID, Business, Scale, State, Ecoll, Efacility, Efan, Gfacility, Eheat, Gheat, Einterequip, Ginterequip, Einterlight, Gwater) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?)")
    count = 0

    # batch insert into cassandra database
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for record in iter:
        batch.add(insert_statement,(record[0], record[1][0], record[1][1], record[1][2], record[1][3], record[1][4],record[1][5], record[1][6], record[1][7], record[1][8], record[1][9], record[1][10], record[1][11], record[1][12]))

    #split the batch, so th4at the batch will not exceed the size limit
        count += 1
        if count % 200== 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    print "saved"
    session.execute(batch)
    session.shutdown()


def getAvg(tuples): # get the average value for each parameter of each rdd for any user ID
    
    key0 = datetime.datetime.now().strftime ("%Y-%m-%d %H:%M:%S")
    key1 = tuples[1][0]
    key2 = tuples[1][1]
    key3 = tuples[1][2]
    num1 = tuples[1][3]
    num2 = tuples[1][4]
    num3 = tuples[1][5]
    num4 = tuples[1][6]
    num6 = tuples[1][8]
    num7 = tuples[1][9]
    num8 = tuples[1][10]
    num9 = tuples[1][11]
    num10 = tuples[1][12]
    num11 = tuples[1][13]
    n = tuples[1][14]
    avg1 = num1/ n
    avg2 = num2/ n
    avg3= num3/ n
    avg4 = num4/ n
    avg6 = num6/ n
    avg7 = num7/ n
    avg8 = num8/ n
    avg9 = num9/ n
    avg10 = num10/ n
    avg11 = num11/ n


    return (tuples[0], (key1, key2, key3, avg1, avg2, avg3, avg4, avg6, avg7, avg8, avg9, avg10, avg11, key0))

# assign negative values for those missing data points, so that they can be easily filtered out in the later process
def getValue(tuples):
    key = tuples[0]
    key1 = tuples[1]
    key2 = tuples[13]
    key3 = tuples[14]

    for i in range(1, 3):
        x='val' + str(i)
        y='float(tuples['+str(i+1)+ '])'
        try:
          exec("%s = %s" % (x,y))

        except ValueError: 
          exec("%s = %d" % (x, -1.0))

    for i in range(3, 11):
        x='val' + str(i)
        y='float(tuples['+str(i+2)+ '])'
        try:
          exec("%s = %s" % (x,y))

        except ValueError:
          exec("%s = %d" % (x, -1.0))

    try:
        val11=float(tuples[15])
    except ValueError:
        val11=-1.0

    return (key, (key1, key2, key3, val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, 1))


def main():
    
    
    
    sc = SparkContext(appName="PythonSparkStreamingKafka")
    sc.setLogLevel("WARN")

    # set microbatch interval as 5 seconds, this can be customized according to the project
    ssc = StreamingContext(sc, 5)
    # directly receive the data under a certain topic
    kafkaStream = KafkaUtils.createDirectStream(ssc, ['EC'], {"metadata.broker.list": 'ec2-18-236-5-16.us-west-2.compute.amazonaws.com:9092'}) 
    batchdata = kafkaStream.map(lambda x: x[1])
    counts = batchdata.map(lambda line: line.split(',')) # since the input data are csv files, specify the delimeter "," in order to extract the values
    Processed_data = counts.map(getValue).reduceByKey(lambda a, b: (a[0], a[1], a[2], a[3]+ b[3], a[4]+ b[4], a[5]+ b[5], a[6]+ b[6], a[7]+ b[7], a[8]+ b[8], a[9]+ b[9], a[10]+ b[10], a[11]+ b[11], a[12]+ b[12], a[13]+ b[13], a[14]+ b[14])).map(getAvg) 
    

    Processed_data.pprint()
    #State_data.pprint()
    #Recent_data.pprint()
    #Recent_data.pprint()

    Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra)) # save rdd in different format to different tables
    Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandrastate))
    Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandrarecent))
    
    ssc.start()
    ssc.awaitTermination()

    return


if __name__ == '__main__':

    main()

    



