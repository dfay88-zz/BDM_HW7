import csv
from geopy.distance import vincenty
from pyspark.sql import sqlContext, Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import HiveContext
import datetime


def extractBikeTrips(partId, list_of_records):
    if partId==0:
        list_of_records.next()
    reader = csv.reader(list_of_records)
    for row in reader:
        if row[3].split(' ')[0] == '2015-02-01':
            if row[6] == 'Greenwich Ave & 8 Ave':
                yield Row(row[3], row[13])

def extractTaxiTrips(partId, list_of_records):
    if partId==0:
        list_of_records.next()
    reader = csv.reader(list_of_records)
    for row in reader:
        if row[4] and row[5] != 'NULL':
            if vincenty((40.73901691, -74.00263761), (row[4], row[5])).miles <= .25:
                yield row[1]
        else:
            pass

def createTimeWindow(timeString):
    time_beg = datetime.datetime.strptime(timeString, '%Y-%m-%d %H:%M:%S.%f')
    time_end = datetime.datetime.strptime(timeString, '%Y-%m-%d %H:%M:%S.%f')
    time_end += datetime.timedelta(0,600)
    return Row(time_beg.strftime('%Y-%m-%d %H:%M:%S.%f'), time_end.strftime('%Y-%m-%d %H:%M:%S.%f'))

def main(sc):
    spark = HiveContext(sc)

    citibike = sc.textFile('/tmp/citibike.csv')
    taxi = sc.textFile('/tmp/yellow.csv.gz')

    bikeTrips = citibike.mapPartitionsWithIndex(extractBikeTrips)
    taxiTrips = taxi.mapPartitionsWithIndex(extractTaxiTrips).map(createTimeWindow)

    schema = StructType([StructField('drop_off_time_begin', StringType()), StructField('drop_off_time_end', StringType())])
    taxi_df = sqlContext.createDataFrame(taxiTrips, schema)
    taxi_df = taxi_df.select(taxi_df['drop_off_time_begin'].cast('timestamp'), taxi_df['drop_off_time_end'].cast('timestamp'))


    schema = StructType([StructField('time', StringType()), StructField('bikeid', StringType())])
    citibike_df = sqlContext.createDataFrame(bikeTrips, schema)
    citibike_df = citibike_df.select(citibike_df['time'].cast('timestamp'), citibike_df['bikeid'])


    matched_trips = taxi_df.join(citibike_df).filter(taxi_df.drop_off_time_begin <                 citibike_df.time).filter(citibike_df.time < taxi_df.drop_off_time_end).dropDuplicates(['time','bikeid'])


    print matched_trips.count()

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
