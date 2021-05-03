## Avro Schema
```
data/data_schema.json
```

```json
{
	"type": "record",
	"doc": "This event records routes between airports on airlines.",
	"name": "AirlineRouteEvent",
	"fields": [{
		"name": "id",
		"type": "string",
		"doc": "A universally unique identifier that is generated using random numbers"
	}, {
		"name": "datetime",
		"type": "string",
		"doc": "The produced event datetime in UTC format"
	}, {
		"name": "airline",
		"type": "string",
		"doc": "2-letter (IATA) or 3-letter (ICAO) code of the airline"
	}, {
		"name": "airline_id",
		"type": "int",
		"doc": "Unique OpenFlights identifier for airline"
	}, {
		"name": "source_airport",
		"type": "string",
		"doc": "3-letter (IATA) or 4-letter (ICAO) code of the source airport"
	}, {
		"name": "source_airport_id",
		"type": "int",
		"doc": "Unique OpenFlights identifier for source airport"
	}, {
		"name": "destination_airport",
		"type": "string",
		"doc": "3-letter (IATA) or 4-letter (ICAO) code of the destination airport"
	}, {
		"name": "destination_airport_id",
		"type": "int",
		"doc": "Unique OpenFlights identifier for destination airport"
	}, {
		"name": "codeshare",
		"type": "boolean",
		"doc": "True if this flight is a codeshare (that is, not operated by Airline, but another carrier)"
	}, {
		"name": "stops",
		"type": "int",
		"doc": "Number of stops on this flight (0 for direct)"
	}, {
		"name": "equipment",
		"type": "string",
		"doc": "3-letter codes for plane type(s) generally used on this flight, separated by spaces"
	}]
}
```

## Spark structured streaming
1. Launch the Kafka environment:
```
docker-compose -f ./kafka/docker-compose.yml up -d
```
2. Build Spark container with Flask app:
```
​docker build -t spark-chief -f Dockerfile .
```
3. Create common network and connect the Kafka broker:
```
docker network create kafka-net
docker network connect kafka-net broker
```
4. Run Spark container
```
docker run --rm -p 5000:5000 -it --name spark_chief spark-chief sh
```
5. In a separate terminal, connect the Spark container to the common network.
```
docker network connect kafka-net spark_chief
```
6. Produce data

[http://0.0.0.0:5000/produce](http://0.0.0.0:5000/produce)
```json
{
	"data": [{
		"Total count": 67663
	}, {
		"Total count (%)": 100.0
	}, {
		"Total success": 66765
	}, {
		"Total success (%)": 98.67
	}, {
		"Total errors by var transform": 898
	}, {
		"Total errors by var transform (%)": 1.33
	}, {
		"Total errors by invalid schema": 0
	}, {
		"Total errors by invalid schema (%)": 0.0
	}],
	"error": false,
	"message": "produce",
	"statusCode": 200
}
```

7. Run different views

[http://0.0.0.0:5000/top10_source_airport](http://0.0.0.0:5000/top10_source_airport)
```json
{
	"data": [{
		"count": 933,
		"source_airport": "ATL"
	}, {
		"count": 564,
		"source_airport": "ORD"
	}, {
		"count": 547,
		"source_airport": "PEK"
	}, {
		"count": 525,
		"source_airport": "LHR"
	}, {
		"count": 524,
		"source_airport": "CDG"
	}, {
		"count": 497,
		"source_airport": "FRA"
	}, {
		"count": 495,
		"source_airport": "LAX"
	}, {
		"count": 486,
		"source_airport": "CTU"
	}, {
		"count": 469,
		"source_airport": "DFW"
	}, {
		"count": 459,
		"source_airport": "AMS"
	}],
	"error": false,
	"message": "top10_source_airport",
	"statusCode": 200
}
```

[http://0.0.0.0:5000/top10_equipment_30min_window](http://0.0.0.0:5000/top10_equipment_30min_window)
```json
{
	"data": [{
		"count": 5115,
		"end": "2021-05-03 10:18:30",
		"equipment": "738",
		"start": "2021-05-03 10:18:00"
	}, {
		"count": 4169,
		"end": "2021-05-03 10:19:00",
		"equipment": "320",
		"start": "2021-05-03 10:18:30"
	}, {
		"count": 4117,
		"end": "2021-05-03 10:18:30",
		"equipment": "320",
		"start": "2021-05-03 10:18:00"
	}, {
		"count": 1951,
		"end": "2021-05-03 10:19:00",
		"equipment": "738",
		"start": "2021-05-03 10:18:30"
	}, {
		"count": 1592,
		"end": "2021-05-03 10:19:00",
		"equipment": "319",
		"start": "2021-05-03 10:18:30"
	}, {
		"count": 1356,
		"end": "2021-05-03 10:18:30",
		"equipment": "319",
		"start": "2021-05-03 10:18:00"
	}, {
		"count": 1159,
		"end": "2021-05-03 10:19:00",
		"equipment": "737",
		"start": "2021-05-03 10:18:30"
	}, {
		"count": 1077,
		"end": "2021-05-03 10:18:30",
		"equipment": "73H",
		"start": "2021-05-03 10:18:00"
	}, {
		"count": 963,
		"end": "2021-05-03 10:19:00",
		"equipment": "73H",
		"start": "2021-05-03 10:18:30"
	}, {
		"count": 950,
		"end": "2021-05-03 10:18:30",
		"equipment": "737",
		"start": "2021-05-03 10:18:00"
	}],
	"error": false,
	"message": "top10_equipment_30min_window",
	"statusCode": 200
}
```

[http://0.0.0.0:5000/dump_table](http://0.0.0.0:5000/dump_table)
```json
{
    "data": [{
        "airline": "2B",
        "airline_id": 410,
        "codeshare": false,
        "datetime": "Mon, 03 May 2021 08:22:26 GMT",
        "destination_airport": "KZN",
        "destination_airport_id": 2990,
        "equipment": "CR2",
        "id": "d89ac153-e415-472f-8fd8-785c8491395d",
        "source_airport": "AER",
        "source_airport_id": 2965,
        "stops": 0
    },
    ...
    ...
    ...
    , {
        "airline": "ZM",
        "airline_id": 19016,
        "codeshare": false,
        "datetime": "Mon, 03 May 2021 10:18:52 GMT",
        "destination_airport": "FRU",
        "destination_airport_id": 2912,
        "equipment": "734",
        "id": "040c2d3b-20ca-4db5-8ef7-4d10a7a40a00",
        "source_airport": "OSS",
        "source_airport_id": 2913,
        "stops": 0
    }],
    "error": false,
    "message": "dump_table",
    "statusCode": 200
}
```
