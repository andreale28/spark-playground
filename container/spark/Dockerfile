FROM bitnami/spark:3.4

# Run installation tasks as root
USER root

RUN apt-get update && apt-get install -y curl
# Project folder name
ARG PROJECT_FOLDER=/opt/spark-playground

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

RUN mkdir -p ${SPARK_HOME}/hms-3.0.0/jars
COPY conf/hms-3.0.0-deps.txt ${SPARK_HOME}/hms-3.0.0/jars
RUN cd ${SPARK_HOME}/hms-3.0.0/jars && xargs -n1 -P1 curl --remote-name < hms-3.0.0-deps.txt

#COPY conf/hive-site.xml ${SPARK_HOME}/hms-3.0.0/conf
#RUN #useradd -d /home/sparkuser -ms /bin/bash sparkuser

COPY ./requirements.txt ${PROJECT_FOLDER}/requirements.txt
RUN python -m pip install --no-cache-dir -r ${PROJECT_FOLDER}/requirements.txt

# Copy local directory into container app directory
WORKDIR ${PROJECT_FOLDER}
COPY . ${PROJECT_FOLDER}

#RUN chown -R sparkuser:sparkuser ${PROJECT_FOLDER}
#USER sparkuser
