from pyspark import SparkContext
from pyspark.sql.functions import expr
from pyspark.sql import Row, HiveContext

def parsebikeCSV(idx, records):
    import csv
    if idx == 0:
        records.next()
    for row in csv.reader(records):
        if row[6] == 'Greenwich Ave & 8 Ave':
            yield Row(bikeid=row[13], time=row[3])

def parsetaxiCSV(idx, records):
    import csv
    import pyproj
    import numpy as np

    p = pyproj.Proj(proj='utm', zone=18, ellps='WGS84')

    if idx == 0:
        records.next()

    for row in csv.reader(records):
        if row[5] == 0 or row[5] == 'NULL': # There are messed up rows.                                  
            pass
        else:
            x, y = p(row[5], row[4])

            # Calculate distance from citibike station                                                   
            dist = np.sqrt((x-584210.3291341639)**2 + (y-4510264.471325981)**2)
            dist = round(dist, 4)
            yield Row(time=row[1], dist=dist)

def main(sc):
    spark = HiveContext(sc)

    bike_rows = sc.textFile('/tmp/citibike.csv').mapPartitionsWithIndex(parsebikeCSV)
    bike_rows = bike_rows.filter(lambda x: x[1] < '2015-02-02')

    bike_df = bike_rows.toDF(['bikeid', 'time'])
    bike_df = bike_df.withColumn('time', bike_df.time.cast('timestamp'))

    cab_rows = sc.textFile('/tmp/yellow.csv.gz').mapPartitionsWithIndex(parsetaxiCSV)
    cab_rows = cab_rows.filter(lambda x: x[0] < 402.336)

    cab_df = cab_rows.toDF(['dist', 'time'])
    cab_df = cab_df.withColumn('time', cab_df.time.cast('timestamp'))
    cab_df = cab_df.withColumn('time10', cab_df.time + expr('INTERVAL 10 MINUTES'))

    bike_df.registerTempTable('bike_table')
    cab_df.registerTempTable('cab_table')
    results = spark.sql("SELECT DISTINCT bike_table.bikeid, bike_table.time FROM bike_table JOIN cab_table ON cab_table.time < bike_table.time and  bike_table.time < cab_table.time10")

    print results.count()

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)