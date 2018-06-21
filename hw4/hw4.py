from __future__ import print_function
import pyspark
from pyspark.sql import SQLContext
from pyspark.mllib.regression import LabeledPoint
import sys
import os
import numpy as np
from operator import itemgetter
from pyspark.mllib.tree import RandomForest, RandomForestModel, DecisionTree, DecisionTreeModel
from pyspark.ml.feature import StringIndexer, StringIndexerModel
from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

os.environ['PYSPARK_PYTHON'] = 'python2.6'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-csv_2.11:1.5.0 pyspark-shell'

print(sys.version_info)
# $SPARK_HOME/bin/spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master local hw4.py
# $SPARK_HOME/bin/spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master yarn --deploy-mode client hw4.py

FEATURE_USED = ['Month',
                'DayofMonth',
                'DayOfWeek',
                'CRSDepTime',
                'CRSArrTime',
                'UniqueCarrier',
                'FlightNum',
                'CRSElapsedTime',
                'Origin',
                'Dest',
                'Distance',
                'Cancelled']

def custom_zip(rdd1, rdd2):
    index = itemgetter(1)
    def prepare(rdd, npart):
        rdd = rdd.zipWithIndex()
        rdd = rdd.sortByKey(index, numPartitions=npart)
        rdd = rdd.keys()
        return rdd

    npart_rdd1 = rdd1.getNumPartitions()
    npart_rdd2 = rdd2.getNumPartitions()
    npart = npart_rdd1 + npart_rdd2

    prepared_rdd1 = prepare(rdd1, npart)
    prepared_rdd2 = prepare(rdd2, npart)
    ziped = prepared_rdd1.zip(prepared_rdd2)
    return ziped

def load_csv(sc, filename='200[0-5].csv'):
    sql_context = SQLContext(sc)
    df = sql_context.read.option('mode', 'PERMISSIVE')\
                            .load(filename,
                            format='com.databricks.spark.csv',
                            header='true',
                            nullValue='NA',
                            inferSchema='true').cache()
    df = df[FEATURE_USED]
    df = df.na.drop()
    # turn string to index
    for col in ['UniqueCarrier', 'Origin', 'Dest']:
        df = StringIndexer(inputCol=col, outputCol=col+'_value').fit(df).transform(df)
        df = df.drop(col)

    # reordering
    df = df.select(['Month',
                    'DayofMonth',
                    'DayOfWeek',
                    'CRSDepTime',
                    'CRSArrTime',
                    'UniqueCarrier_value',
                    'FlightNum',
                    'CRSElapsedTime',
                    'Origin_value',
                    'Dest_value',
                    'Distance',
                    'Cancelled'])
    return df

def get_labeled_points(df):
    return df.rdd.map(lambda row: [e for e in row]).map(lambda row: LabeledPoint(row[-1], row[:-1]))

def downsample(data):
    counts = data.groupBy('Cancelled').count().take(2)
    threshold = float(counts[1][1]) / float(counts[0][1])
    threshold = threshold if threshold < 1. else 1./threshold
    print('threshold:', threshold)
    data = data.sampleBy('Cancelled', fractions={0: threshold, 1: 1.}, seed=999)
    return data

def get_rf_model(sc, train=None):
    model_path = 'rf.model'
    if train is None:
        model = RandomForestModel.load(sc, model_path)
    else:
        model = RandomForest.trainClassifier(
                                train,
                                numClasses=2,
                                numTrees=10,
                                categoricalFeaturesInfo={},
                                featureSubsetStrategy="auto",
                                impurity='gini',
                                maxDepth=10, maxBins=100)
        model.save(sc, model_path)

    return model

def get_dt_model(sc, train=None):
    model_path = 'dt.model'
    if train is None:
        model = DecisionTreeModel.load(sc, model_path)
    else:
        model = DecisionTree.trainClassifier(
                                train,
                                numClasses=2,
                                categoricalFeaturesInfo={},
                                impurity='gini',
                                maxDepth=10)
        model.save(sc, model_path)

    return model

def evaluate(sc, model, labeled_points):
    labeled_points = labeled_points.union(sc.parallelize([]))
    labels = labeled_points.map(lambda lp: lp.label)
    predictions = model.predict(labeled_points.map(lambda lp: lp.features))
    predictions, labels = predictions.union(sc.parallelize([])), labels.union(sc.parallelize([]))
    predictionAndLabels = predictions.zip(labels)

    # Instantiate metrics object
    metrics = MulticlassMetrics(predictionAndLabels)

    # Overall statistics
    cm = metrics.confusionMatrix().toArray()
    recall = cm[1][1] / (cm[1][0] + cm[1][1])
    precision = cm[1][1] / (cm[0][1] + cm[1][1])
    f1Score = 2. * (precision * recall) / (precision + recall)
    print("Summary Stats")
    print("Precision = %s" % precision)
    print("Recall = %s" % recall)
    print("F1 Score = %s" % f1Score)
    print("Confusion Matrix = ")
    print("    0        1")
    print("0   {0}      {1}".format(cm[0][0], cm[1][0]))
    print("1   {0}      {1}".format(cm[0][1], cm[1][1]))

if __name__ == '__main__':
    sc = pyspark.SparkContext(appName='hw4')

    if sys.argv[2] == 'rf':
        print('RandomForest')
        get_model = get_rf_model
    else:
        print('DecisionTree')
        get_model = get_dt_model

    if sys.argv[1] == 'train':
        train_df = load_csv(sc, filename='200[0-5].csv')
        train_df = downsample(train_df)
        train, valid = get_labeled_points(train_df).randomSplit([0.7, 0.3], seed=999)
        model = get_model(sc, train=train)
    else: # test
        model = get_model(sc, train=None)

        test_df = load_csv(sc, filename='2006.csv')
        test = get_labeled_points(test_df)

        test_df_downsampled = downsample(test_df)
        test_downsampled = get_labeled_points(test_df_downsampled)

        print('\nTesting Evaluation')
        evaluate(sc, model, test)

        print('\nTesting Evaluation - Downsampled')
        evaluate(sc, model, test_downsampled)
