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

def sendCassandra(iter):
    print("send to cassandra")
    cluster = Cluster(['52.13.146.195', '54.71.37.147', '52.40.168.83', '54.245.27.74'])
    session = cluster.connect()
    session.execute('USE ' + "Energy_consumption")
    insert_statement = session.prepare("INSERT INTO rawdata (Time, ID, Business, Scale, State, Ecoll, Efacility, Efan, Gfacility, Eheat, Gheat, Einterequip, Ginterequip, Einterlight, Gwater) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    count = 0

    # batch insert into cassandra database
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for record in iter:
        batch.add(insert_statement,(record[1][13], record[0], record[1][0], record[1][1], record[1][2], record[1][3], record[1][4],record[1][5], record[1][6], record[1][8], record[1][9], record[1][10], record[1][11], record[1][12]))


    #split the batch, so that the batch will not exceed the size limit
     #   count += 1
      #  if count % 500== 0:
       #     session.execute(batch)
        #    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    print "saved"


def sendCassandrastate(iter):
    print("send to cassandra")
    cluster = Cluster(['52.13.146.195', '54.71.37.147', '52.40.168.83', '54.245.27.74'])
    session = cluster.connect()
    session.execute('USE ' + "Energy_consumption")
    insert_statement = session.prepare("INSERT INTO statedata (Time, State, Ecoll, Efacility, Efan, Gfacility, Eheat, Gheat, Einterequip, Ginterequip, Einterlight, Gwater, Etotal, Gtotal) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?,?)")
    count = 0

    # batch insert into cassandra database
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    for record in iter:
        batch.add(insert_statement,(record[0], record[1], record[2], record[3], record[4],record[5], record[6], record[7], record[8], record[9], record[10], record[11], record[12], record[13]))



    #split the batch, so that the batch will not exceed the size limit
     #   count += 1
      #  if count % 500== 0:
       #     session.execute(batch)
        #    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    print "saved"
            

# send the batch that is less than 500            
    session.execute(batch)
    session.shutdown()



def getAvg(tuples):
    key0 = datetime.datetime.now().strftime ("%Y-%m-%d %H:%M:%S")
    key1 = tuples[1][0]
    key2 = tuples[1][1]
    key3 = tuples[1][2]
    num1 = tuples[1][3]
    num2 = tuples[1][4]
    num3 = tuples[1][5]
    num4 = tuples[1][6]
    num5 = tuples[1][7]
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


def getTotal(tuples):
    key1 = tuples[1][2]
    num1 = tuples[1][3]
    num2 = tuples[1][4]
    num3 = tuples[1][5]
    num4 = tuples[1][6]
    num5 = tuples[1][7]
    num6 = tuples[1][8]
    num7 = tuples[1][9]
    num8 = tuples[1][10]
    num9 = tuples[1][11]
    num10 = tuples[1][12]
    key2 =tuples[1][13]
    num11 =num1+num2+num3+num5+num7+num9
    num12 =num4+num6+num8+num10

    


    return (key2, key1, num1, num2, num3, num4, num5, num6, num7, num8, num9, num10, num11, num12)



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
          exec("%s = %d" % (x, 0.0))

    for i in range(3, 11):
        x='val' + str(i)
        y='float(tuples['+str(i+2)+ '])'
        try:
          exec("%s = %s" % (x,y))

        except ValueError:
          exec("%s = %d" % (x, 0.0))

    try:
        val11=float(tuples[15])
    except ValueError:
        val11=0.0

    return (key, (key1, key2, key3, val1, val2, val3, val4, val5, val6, val7, val8, val9, val10, val11, 1))

def split_line(line):
    '''
    This function splits line efficiently using csv package
    '''
    f = io.StringIO(line)
    return reader.next()


def main():
    sc = SparkContext(appName="PythonSparkStreamingKafka")

    #SparkContext.textFile("/home/ubuntu/test/data/day").flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).saveAsTextFile("zzz")

    sc.setLogLevel("WARN")

    # set microbatch interval as 10 seconds
    ssc = StreamingContext(sc, 10)
    #ssc.checkpoint(config.CHECKPOINT_DIR)

    # create a direct stream from kafka without using receiver

    kafkaStream = KafkaUtils.createDirectStream(ssc, ['EC'], {"metadata.broker.list": 'localhost:9092'})
    batchdata = kafkaStream.map(lambda x: x[1])
    counts = batchdata.map(lambda line: line.split(','))
    #batchdata1 = split_line(batchdata)
    Processed_data = counts.map(getValue).reduceByKey(lambda a, b: (a[0], a[1], a[2], a[3]+ b[3], a[4]+ b[4], a[5]+ b[5], a[6]+ b[6], a[7]+ b[7], a[8]+ b[8], a[9]+ b[9], a[10]+ b[10], a[11]+ b[11], a[12]+ b[12], a[13]+ b[13], a[14]+ b[14])).map(getAvg) 
    State_data=Processed_data.map(getTotal)
    #BatchDF = Processed_data.map(lambda x: Row(ID=x[0], Business=x[1][0], Scale=x[1][1], State=x[1][2], Ecool=x[1][3], Efacility=x[1][4],Efan=x[1][5], Gfaciliy=x[1][6], EHeat=x[1][7], GHeat=x[1][8], EInterEqu=x[1][9], GInterEqu=x[1][10], EInterlight=x[1][11], Water=x[1][12])).createDataFrame(Energy)
    
    #coords = lines.map(lambda line: line)
    #coords.foreachRDD(save_line)
    #coords.pprint()

    #lines.pprint()
    #counts = lines.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    #words = lines.flatMap(lambda line: line.split(','))
    #counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    counts.pprint()

    Processed_data.pprint()
    State_data.pprint()


    Processed_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))
    State_data.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandrastate))
    ssc.start()
    ssc.awaitTermination()

    return


if __name__ == '__main__':
    main()




