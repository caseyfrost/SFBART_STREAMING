# SFBART DELAY STREAMING


## Background
This project streams San Francisco Bay Area Rapid Transit (SFBART) General Transit Feed Specification (GTFS) and Twitter data to create a database with average delays per SFBART line and tweets related to SFBART delays.

The data for SFBART delays and lines comes from [(GTFS)](https://developers.google.com/transit/gtfs), which is a standard for sharing transit data. The tweets come from Twitter's API V2, and tweepy.

The data is streamed via python and kafka, transformed via python and ksqldb, and stored via python, kafka connect, and postgres.

## How it works
The yaml file is configured to create the standard services required to stand up a kafka, ksqldb, and postgres environment. Those configurations are well documented online so I will not go over them here. The custom images are the interesting part of how the application works and do require some explanation. 

The application starts with the BART GTFS producer and the Twitter producer. The GTFS producer takes the real time feed from BART's GTFS API, normalized the nested json response into a flatter structure, and adds additional details to the data from BART's static GTFS files. The real time feed provides coded information for the station name, a trip_id, and the delay information. The aditional details provided by the static files are the full name of the stops and the lines the real time trip is on. To combine this data I needed to take the trip_id from the real time data, and scan the static data for that corresponding ID. The GTFS producer script then takes the message and adds it to a kafka topic. The twitter producer simply creates a stream that is searching for tweets related to BART delays and adds them to a kafka topic.

Next, ksqldb and a python library called ksql is used to create the window function to find the average delay by line. The python library ksql allows my script to send commands to ksql's api. First, the ksqldb script takes the kafka topics with json messages from the producers and creates new avro streams with schemas. The schemas are necessary for the Postgres sink connectors. At this point the twitter stream is ready for the Postgres sink connector but the BART GTFS data still needs more manipulation. The next step is to take the BART GTFS stream and convert it into a talbe using a window function. The script uses a hopping window query to create hour long windows 15 minutes apart and find the average delay for each line within those windows. 

Finally, the data is sent to postgres via Postgres sink connectors. The ksqldb script creates two sink connectors. One for the Twitter data, and one for the BART GTFS data. The BART GTFS data does not use the record_key configuration because the window function adds non UTF-8 characaters to the kafka message key.

After all of that the data is streaming directly from Twitter and BART's GTFS feed into your Postgres database!


## Requirements
1. Docker
2. Twitter developer account/app and associated keys
3. Static GTFS files

## Steps to run on your own
1. Download the repo to your personal computer
2. Download the latest trips and stops txt files from [https://www.bart.gov/dev/schedules/google_transit.zip](https://www.bart.gov/dev/schedules/google_transit.zip)
3. Replace the existing stop and trips txt files in \bart_gtfs_producer\app\ with your newly downloaded files
4. Add your Twitter app API keys to config file
4. Start Docker on your computer
5. Build bart_gtfs_producer, ksql, and twitter_producer by navigating to those directorie and running: docker build -t {name of directory}:latest -f Dockerfile app
6. Navigate to the SFBART_STREAMING directory and run: docker-compose up --scale additional-ksqldb-server=0
7. Here I have had difficulties if the ksqldb server does not start fully or the kafka connect server does not start fully. If either do not start fully you will need to restart their containers. I think I need to build a health check here that waits for kafka to be fully running.
8. Wait for 


## Future enhancements
1. Health check for kafka server, ksqldb server, and kafka connect server

