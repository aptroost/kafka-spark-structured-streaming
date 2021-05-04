import os
from time import sleep

import findspark
import kafka
from flask import jsonify
from pyspark.sql.functions import col, explode, from_json, split, window
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (BooleanType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

from . import create_app

findspark.init()

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 pyspark-shell'

app = create_app()

# Start session with Spark
spark = SparkSession \
    .builder \
    .appName("routeData").getOrCreate()

# Read the data from kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "routes_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Print out the dataframa schema
# df.printSchema()

# Convert the datatype for value to a string
string_df = df.selectExpr("CAST(value AS STRING)")

# Print out the new dataframa schema
# string_df.printSchema()

# Create a schema for the df
schema = StructType([
    StructField("id", StringType()),
    StructField("datetime", TimestampType()),
    StructField("airline", StringType()),
    StructField("airline_id", IntegerType()),
    StructField("source_airport", StringType()),
    StructField("source_airport_id", IntegerType()),
    StructField("destination_airport", StringType()),
    StructField("destination_airport_id", IntegerType()),
    StructField("codeshare", BooleanType()),
    StructField("stops", IntegerType()),
    StructField("equipment", StringType())
])

# Select the data present in the column value and apply the schema on it
json_df = string_df \
    .withColumn("jsonData",
                from_json(col("value"),
                          schema)) \
    .select("jsondata.*")

# Print out the dataframa schema
# json_df.printSchema()


def run_query(q, name, outputMode, outputFormat):
    import uuid

    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from IPython.display import clear_output, display
    from pyspark.sql.functions import desc

    unique_query_name = str(uuid.uuid4()).replace('-', '')
    query = q \
        .writeStream \
        .outputMode(outputMode) \
        .format(outputFormat) \
        .queryName(unique_query_name) \
        .start()

    while True:
        sleep(1)
        clear_output(wait=True)
        display(query.status)

        spark_query = spark.sql('SELECT * FROM ' + unique_query_name)
        df_pandas = spark_query.toPandas()
        df_pandas.to_csv(os.environ['SPARK_HOME'] + '/results/' + name + '.csv', index=False)

        if len(df_pandas.index) > 0:
            return df_pandas


@app.route('/')
def home():
    return jsonify(message="Welcome",
                   error=False,
                   statusCode=200), 200


@app.route('/top10_source_airport')
def top10_source_airport():
    q = json_df.select("*")\
        .groupBy(
        json_df.source_airport
    )\
        .count()\
        .orderBy(
        col('count').desc()
    )\
        .limit(10)

    df_ = run_query(q, "top10_source_airport", "complete", "memory")
    df_dict = df_.to_dict(orient='records')

    return jsonify(message="top10_source_airport",
                   error=False,
                   data=df_dict,
                   statusCode=200), 200


@app.route('/dump_table')
def dump_table():
    q = json_df.select("*")

    df_ = run_query(q, "dump_table", "update", "memory")
    df_dict = df_.to_dict(orient='records')

    return jsonify(message="dump_table",
                   error=False,
                   data=df_dict,
                   statusCode=200), 200


@app.route('/top10_equipment_30min_window')
def top10_equipment_30min_window():
    q = json_df.select("*")\
        .groupBy(
        window(
            json_df.datetime,
            "30 seconds",
            "30 seconds"),
        json_df.equipment
    )\
        .count()\
        .orderBy(
        col('count').desc(),
        col('equipment').desc()
    )\
        .limit(10)

    df_ = run_query(q, "top10_equipment_30min_window", "complete", "memory")
    for idx, row in df_.iterrows():
        df_.at[idx, 'start'] = row['window'][0]
        df_.at[idx, 'end'] = row['window'][1]
    df_.pop("window")
    df_['start'] = df_['start'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_['end'] = df_['end'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df_dict = df_.to_dict(orient='records')

    return jsonify(message="top10_equipment_30min_window",
                   error=False,
                   data=df_dict,
                   statusCode=200), 200


@app.route('/produce')
def produce():
    import avro_validator
    import json
    import uuid
    import datetime
    from kafka import KafkaProducer
    import os

    # Local file that will be send to Kafka row by row
    filename_data = os.environ['SPARK_HOME'] + '/data/routes.dat'
    filename_schema = os.environ['SPARK_HOME'] + '/data/data_schema.json'

    # print(str(filename_data) + ' \t\t' + str(os.path.getsize(filename_data)) + ' bytes')
    # print(str(filename_schema) + ' \t' + str(os.path.getsize(filename_schema)) + ' bytes')

    # Open data schema in external file
    # avro schema style source: https://avro.apache.org/docs/current/spec.html
    with open(filename_schema) as json_file:
        data_schema = json.load(json_file)
    # data_schema

    # Create Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    # Read local file
    open_data = open(filename_data, 'r')
    lines = open_data.readlines()

    # Validate data and send row by row to Kafka
    def strToBool(codeshare):
        if codeshare == 'Y':
            return True
        else:
            return False

    count_error_transform_var = 0
    count_error_invalid_schema = 0
    count_success = 0
    count_total = 0

    # clear file
    f = open(os.environ['SPARK_HOME'] + "/results/error.txt", "w")
    f.close()

    # for line in lines[0:1000]:
    for line in lines:
        count_total += 1
        line = line.replace('\n', '')
        listOfStr = line.strip().split(',')

        try:
            # Pragmatic approach
            line_dict = {
                'id':                      str(uuid.uuid4()),
                'datetime':                str(datetime.datetime.utcnow()),
                'airline':                 str(listOfStr[0]),
                'airline_id':              int(listOfStr[1]),
                'source_airport':          str(listOfStr[2]),
                'source_airport_id':       int(listOfStr[3]),
                'destination_airport':     str(listOfStr[4]),
                'destination_airport_id':  int(listOfStr[5]),
                'codeshare':               strToBool(listOfStr[6]),
                'stops':                   int(listOfStr[7]),
                'equipment':               str(listOfStr[8]),
            }
            parsed_schema = avro_validator.schema.Schema(
                json.dumps(data_schema)).parse()
            if parsed_schema.validate(line_dict):
                producer.send("routes_data", value=line_dict)
                count_success += 1
            else:
                f = open(os.environ['SPARK_HOME'] + "/results/error.txt", "a")
                f.write('Schema invalid: ' + line + '\n')
                f.close()
                count_error_invalid_schema += 1
        except:
            f = open(os.environ['SPARK_HOME'] + "/results/error.txt", "a")
            f.write('A variable type transformation failed: ' + line + '\n')
            f.close()
            count_error_transform_var += 1
            pass

    summary = [
        {'Total count': count_total},
        {'Total count (%)': round(count_total/count_total*100, 2)},
        {'Total success': count_success},
        {'Total success (%)': round(count_success/count_total*100, 2)},
        {'Total errors by var transform': count_error_transform_var},
        {'Total errors by var transform (%)': round(
            count_error_transform_var/count_total*100, 2)},
        {'Total errors by invalid schema': count_error_invalid_schema},
        {'Total errors by invalid schema (%)': round(
            count_error_invalid_schema/count_total*100, 2)}
    ]

    return jsonify(message="produce",
                   error=False,
                   data=summary,
                   statusCode=200), 200
