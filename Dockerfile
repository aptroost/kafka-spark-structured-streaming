# Base image to start from.
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# Update the system.
RUN apt-get update \
  && apt-get install -qq -y curl vim net-tools \
  && rm -rf /var/lib/apt/lists/*

# Install Python
RUN apt-get update \
  && apt-get install -y python3-pip \
  && ln -s /usr/bin/python3 /usr/bin/python \
  && rm -rf /var/lib/apt/lists/*

# Install Java
RUN apt-get update \
  && apt-get install -y tzdata \
  && apt-get install -y openjdk-11-jre \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Install Spark
RUN apt-get update -y \
  && apt-get install -y curl \
  && curl https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz -o spark.tgz \
  && tar -xf spark.tgz \
  && mv spark-3.0.0-bin-hadoop2.7 /opt/spark/ \
  && rm spark.tgz

# Set Spark environment
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# SET WORKING DIR
WORKDIR $SPARK_HOME

# Copy files
COPY log4j.properties $SPARK_HOME/conf
COPY data $SPARK_HOME/data/
COPY app $SPARK_HOME/app/
# COPY results $SPARK_HOME/results/
COPY requirements.txt /

# INSTALL PY REQUIREMENTS
RUN pip3 install -r /requirements.txt

ENV FLASK_APP=$SPARK_HOME/app/app.py

ENV PORT 5000
# EXPOSE CONTAINER PORTS
EXPOSE 4040 5000 6066 7077 8080

# Run commands
CMD ["bin/spark-class", \
  "org.apache.spark.deploy.master.Master", \
  "org.apache.spark.deploy.worker.Worker"]

ENTRYPOINT flask run -h 0.0.0.0 -p 5000
