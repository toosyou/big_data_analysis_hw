from __future__ import print_function
import pyspark
from pyspark.sql import SQLContext
import sys
import re
import string
import os

os.environ['PYSPARK_PYTHON'] = 'python2.6'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-csv_2.11:1.5.0 pyspark-shell'

print(sys.version_info)

def remove_non_printable(s):
    return ''.join(filter(lambda x: x in string.printable, s))

def q1(sc, filename='/home_i1/s31tsm77/Youvegottofindwhatyoulove.txt'):
    text = sc.textFile('file://'+filename).map(lambda line: remove_non_printable(line))
    line_counts = text.flatMap(lambda line: [c for c in line]) \
                        .map(lambda char: 1 if re.search('\.|\!|\?', char) else 0) \
                        .reduce(lambda a, b: a+b)

    counts = text.flatMap(lambda line: re.split(';|,|\*|\n|\?|\!| |\.', line.lower()) ) \
                        .filter(lambda word: word != '') \
                        .map(lambda word: (word, (1, 1./line_counts))) \
                        .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
                        .sortBy(lambda a: a[1], ascending=False)


    print('total number of lines:', line_counts, '\n')
    print('{0:<20}{1:<20}{2:<20}\n'.format('word', 'counts', 'average_occurrences'))
    for w, c in counts.take(30):
        print('{0:<20}{1:<20}{2:<20}'.format(w, c[0], c[1]))

def q2(sc, filename='/home_i1/s31tsm77/yellow_tripdata_2017-09.csv'):
    sql_context = SQLContext(sc)
    df = sql_context.read.option('mode', 'DROPMALFORMED')\
                            .load('file://' + filename,
                            format='com.databricks.spark.csv',
                            header='true',
                            inferSchema='true').cache()

    df = df[df.payment_type == 1]
    df = df[['total_amount', 'passenger_count']]
    df = df.groupBy('passenger_count').avg()
    df.show(df.count())

if __name__ == '__main__':
    sc = pyspark.SparkContext(appName='hw3')
    if sys.argv[1] == 'q1':
        q1(sc)
    elif sys.argv[1] == 'q2':
        q2(sc)
    else:
        print('argv error!')
