#!/bin/bash

# setup spark/python to run in jupyter notebook
# - (Galvanize Data Science Immersive 2016)
# 4/11/2017 added export PYSPARK_PYTHON=python3
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=True --NotebookApp.ip='localhost' --NotebookApp.port=8888"

${SPARK_HOME}/bin/pyspark \
--master local[4] \
--executor-memory 1G \
--driver-memory 1G \
--packages com.databricks:spark-csv_2.11:1.5.0 \
--packages com.amazonaws:aws-java-sdk-pom:1.10.34 \
--packages org.apache.hadoop:hadoop-aws:2.7.3
