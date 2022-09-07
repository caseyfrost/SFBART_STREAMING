"""Create kafka producer & topic of real time trip updates from BART real time GTFS API"""
import csv
import json
import time
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson
from kafka import KafkaProducer

# need to be downloaded and placed in the \app directory. Can be found @ https://www.bart.gov/dev/schedules/google_transit.zip
static_trips = 'trips.txt'
static_stops = 'stops.txt'


class BARTRealTime(object):
    # takes the BART data and transforms it into a kafka topic

    def __init__(self):
        self.api_url = r'http://api.bart.gov/gtfsrt/tripupdate.aspx'  # url for the real time BART trip updates
        self.kafka_topic = 'BART_Trips_Normalized'  # name of kafka topic which is needed in the ksql script
        self.kafka_producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 11, 5),  # kafka is the port name for kafka container
                                            value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Will serialize in json
                                            key_serializer=str.encode)  # the kafka message key has to be a string
        
        # dictionary that maps station names to stop_ids
        self.stations = {}
        with open(static_stops) as stations:
            reader = csv.DictReader(stations)
            for row in reader:
                self.stations[row['stop_id']] = row['stop_name']
                
        # dictionary that maps trip_ids to trip headsigns (SFBART Lines)
        self.trips = {}
        with open(static_trips) as trips:
            reader = csv.DictReader(trips)
            for row in reader:
                self.trips[row['trip_id']] = row['trip_headsign']

    def produce_trip_updates(self):
        # uses protobuf to create the trip update from the SFBART API
        # may get an error if you did not install the protobuf correctly or it is corrupted
        # usually re-installing gtfs_realtime_pb2 solves the issue. Must use older version
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(self.api_url)
        feed.ParseFromString(response.content)
        
        # creates the flat message dictionary by parsing the API response
        for entity in feed.entity:
            if entity.HasField('trip_update'):
                update_json = MessageToJson(entity.trip_update)
                trip_update = json.loads(update_json)
                trip_header = trip_update.get('trip')
                if not trip_header or trip_header['scheduleRelationship'] == 'CANCELED':
                    continue
                trip_id = trip_header['tripId']
                stop_time_updates = trip_update.get('stopTimeUpdate')
                if not stop_time_updates or trip_id not in self.trips:
                    continue
                for update in stop_time_updates:
                    if 'arrival' not in update or 'stopId' not in update:
                        continue
                    delay = update['arrival']['delay']
                    stop_id = update['stopId']
                    stop_name = self.stations[stop_id]
                    headsign = self.trips[trip_id]
                    id = f'{trip_id}_{stop_name}'. # creates a unique ID
                    message = {'id': id,
                               'trip_id': trip_id,
                               'delay': delay,
                               'stop_name': stop_name,
                               'headsign': headsign}

                self.kafka_producer.send(self.kafka_topic, message, id)

        self.kafka_producer.flush()

    def run(self):
        while True:
            self.produce_trip_updates()
            time.sleep(60)


def main():
    time.sleep(180)
    test = BARTRealTime()
    test.run()


if __name__ == '__main__':
    main()
