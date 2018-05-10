#!/bin/bash
export PYSPARK_PYTHON=python2.6
time $SPARK_HOME/bin/spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master local hw3.py $1
time $SPARK_HOME/bin/spark-submit --packages com.databricks:spark-csv_2.11:1.5.0 --master yarn --deploy-mode client hw3.py $1

### for spark cluster mode
# spark_pid=$!
# sleep 10
# yarn application -list -appStates RUNNING > tmp.log
# app_id=$(grep `whoami` tmp.log | awk '{print $1'})
# echo $app_id
# wait $spark_pid

# yarn logs -applicationId $app_id
