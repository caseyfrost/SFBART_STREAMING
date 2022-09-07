# SFBART DELAY STREAMING


## Background
This project streams San Francisco Bay Area Rapid Transit (SFBART) General Transit Feed Specification (GTFS) and Twitter data to create a database with average delays per SFBART line and tweets related to SFBART delays.

The data for SFBART delays and lines comes from [(GTFS)](https://developers.google.com/transit/gtfs), which is a standard for sharing transit data. The tweets come from Twitter's API V2, and tweepy.

The data is streamed via python and kafka, transformed via python and ksqldb, and stored via python, kafka connect, and postgres.

## How it works
The yaml file is configured to create the standard services required to stand up a kafka, ksqldb, and postgres environment. Those configurations are well documented online and I do not need to go over them here. The interesting containers 



## Requirements
1. Docker
2. Twitter Developer App and associated keys

## Steps to run on your own


## Futre enhancements

