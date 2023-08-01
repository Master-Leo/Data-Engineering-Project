#!/bin/bash


prefect work-pool create --type docker worker-spark
prefect work-pool create --type docker worker-econ

# Build and push the worker image
docker build -t bruceleo31/woker_spark:latest -f Dockerfile_worker_spark .
docker push bruceleo31/worker_spark:latest

docker build -t bruceleo31/woker_econ:latest -f Dockerfile_worker_econ .
docker push bruceleo31/worker_econ:latest

#start work-pool
docker-compose up 

# Deploy with prefect
prefect deploy --all

prefect deployment -run 'etl-econ-parent-flow/econ-deployment' 

prefect deployment -run 'spark-pipepline/spark-deployment' 
