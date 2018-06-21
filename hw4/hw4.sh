#!/bin/bash
export PYSPARK_PYTHON=python2.6
# export PYSPARK_SUBMIT_ARGS='--packages com.databricks:spark-csv_2.11:1.5.0 pyspark-shell'
$SPARK_HOME/bin/spark-submit --packages com.databricks:spark-csv_2.11:1.5.0\
            --master yarn --deploy-mode client\
            --conf spark.ui.port=4065\
            --conf spark.hadoop.fs.hdfs.impl.disable.cache=true\
            hw4.py train $1

sleep 10s

$SPARK_HOME/bin/spark-submit --packages com.databricks:spark-csv_2.11:1.5.0\
            --master yarn --deploy-mode client\
            --conf spark.ui.port=4065\
            --conf spark.hadoop.fs.hdfs.impl.disable.cache=true\
            hw4.py test $1
# $SPARK_HOME/bin/spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master local --conf spark.ui.port=4065 hw4.py

# for spark cluster mode
# spark_pid=$!
# sleep 10
# yarn application -list -appStates RUNNING > tmp.log
# app_id=$(grep `whoami` tmp.log | awk '{print $1'})
# echo $app_id
# wait $spark_pid

# yarn logs -applicationId $app_id
