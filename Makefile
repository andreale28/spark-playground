

$SPARK_HOME/bin/spark-submit --master local[*] --packages io.delta:delta-core_2.12:2.4.0,io.delta:delta-storage:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
--conf spark.databricks.delta.retentionDurationCheck.enabled=False \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.driver.memory=16g \
--conf spark.driver.cores=4 \
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.access.key=minio \
--conf spark.hadoop.fs.s3a.secret.key=minio123 \
--conf spark.hadoop.fs.s3a.region=us-east-1 \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.path.style.access=true \
workspaces/Hw4/start_etl.py

$SPARK_HOME/bin/spark-shell --packages io.delta:delta-core_2.13:2.4.0,io.delta:delta-storage:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
--conf spark.databricks.delta.retentionDurationCheck.enabled=False \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
--conf spark.driver.memory=16g \
--conf spark.driver.cores=4 \
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.access.key=minio \
--conf spark.hadoop.fs.s3a.secret.key=minio123 \
--conf spark.hadoop.fs.s3a.region=us-east-1 \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.path.style.access=true \

spark-submit --properties-file ./container/spark/spark-defaults.conf ./workspaces/Hw4/start_etl.py
