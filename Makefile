up:
	docker compose up --build -d
down:
	docker compose down
sh:
	docker exec -it spark-master bash
createtable:
	docker exec -ti spark-master bash -c '$$SPARK_HOME/bin/spark-submit --driver-memory 5g --executor-memory 5g --executor-cores 4 --properties-file container/spark/conf/spark-defaults.conf workspaces/scripts/ddl.py'
etl:
	docker exec -ti spark-master bash -c '$$SPARK_HOME/bin/spark-submit --driver-memory 5g --executor-memory 5g --executor-cores 4  --properties-file container/spark/conf/spark-defaults.conf workspaces/scripts/pipeline.py'
start: createtable etl





