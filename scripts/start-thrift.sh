#!/bin/bash
set -e

SPARK_NO_DAEMONIZE=true \
/opt/spark/sbin/start-thriftserver.sh \
  --master local[2] \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.driver.host=0.0.0.0 \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.hive.thriftServer.singleSession=true \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.sql.autoBroadcastJoinThreshold=10485760 \
  --conf spark.driver.memory=512m \
  "$@"
