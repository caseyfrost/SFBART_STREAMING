"""BART KSQLDB class connects to api, creates a stream from the normalized topic, and lastly creates a table that
aggregates records in 2 minute hopping windows of 1 hour each to find the average delay by headsign"""

import time
from ksql import KSQLAPI


class BARTKSQLDBTable(object):

    def __init__(self):
        self.client = KSQLAPI('http://primary-ksqldb-server:8088', timeout=1200)

    def create_stream(self):
        self.client.ksql("CREATE OR REPLACE STREAM BART_GTFS_STREAM (id VARCHAR KEY, trip_id VARCHAR, "
                         "delay BIGINT, stop_name VARCHAR, headsign STRING) WITH ("
                         "kafka_topic='BART_Trips_Normalized', value_format='json')")

    def create_tweet_stream(self):
        self.client.ksql("CREATE OR REPLACE STREAM BART_TWEETS_STREAM (TWEET_ID BIGINT KEY, CREATED_DATE TIMESTAMP, "
                         "TWEET_TEXT STRING) WITH (kafka_topic='BART_TWEETS_STREAM', value_format='json')")

    def create_avro_stream(self):
        self.client.ksql("CREATE OR REPLACE STREAM BART_STREAM_AVRO WITH (VALUE_FORMAT='AVRO', "
                         "KAFKA_TOPIC='BART_STREAM_AVRO') AS SELECT * FROM BART_GTFS_STREAM")

    def create_tweet_avro_stream(self):
        self.client.ksql("CREATE OR REPLACE STREAM BART_TWEET_STREAM_AVRO WITH (VALUE_FORMAT='AVRO', "
                         "KAFKA_TOPIC='BART_TWEETS_STREAM_AVRO') AS SELECT * FROM BART_TWEETS_STREAM")

    def create_table(self):
        tables = self.client.ksql("SHOW TABLES;")
        if "BART_DELAYS" not in tables[0]['tables']:
            self.client.ksql("CREATE TABLE BART_DELAYS WITH (KAFKA_TOPIC='BART_DELAYS', VALUE_FORMAT='AVRO') AS SELECT "
                             "headsign, TOPK(headsign, 1) AS headsign_col, AVG(delay) AS delay, from_unixtime("
                             "WINDOWSTART) as Window_Start, from_unixtime(WINDOWEND) as Window_End, from_unixtime(MAX("
                             "ROWTIME)) as Window_Emit FROM BART_STREAM_AVRO WINDOW HOPPING(SIZE 1 HOURS, ADVANCE BY "
                             "15 MINUTES, RETENTION 1 DAYS, GRACE PERIOD 30 MINUTES) GROUP BY headsign EMIT CHANGES")

    def create_delay_postgres_sink(self):
        connectors = self.client.ksql("SHOW CONNECTORS;")
        if "BART_DELAYS_01" not in connectors[0]['connectors']:
            self.client.ksql("CREATE SINK CONNECTOR BART_DELAYS_01 WITH ("
                             "'connector.class'='io.confluent.connect.jdbc.JdbcSinkConnector',"
                             "'connection.url'='jdbc:postgresql://postgres:5432/default_database?user=postgres&password"
                             "=postgres','topics'='BART_DELAYS',"
                             "'key.converter'='org.apache.kafka.connect.storage.StringConverter',"
                             "'key.converter.schema.registry.url'='http://schema-registry:8081',"
                             "'value.converter'='io.confluent.connect.avro.AvroConverter',"
                             "'value.converter.schema.registry.url'= 'http://schema-registry:8081','pk.mode'='none',"
                             "'pk.fields'='none','Insert.mode'='upsert','auto.create'='true','auto.evolve'='true',"
                             "'table.name.format'='${topic}-01');")

    def create_tweet_postgres_sink(self):
        connectors = self.client.ksql("SHOW CONNECTORS;")
        if "BART_TWEETS_01" not in connectors[0]['connectors']:
            self.client.ksql("CREATE SINK CONNECTOR BART_TWEETS_01 WITH ("
                             "'connector.class'='io.confluent.connect.jdbc.JdbcSinkConnector',"
                             "'connection.url'='jdbc:postgresql://postgres:5432/default_database?user=postgres&password"
                             "=postgres','topics'='BART_TWEETS_STREAM_AVRO',"
                             "'key.converter'='org.apache.kafka.connect.storage.StringConverter',"
                             "'key.converter.schema.registry.url'='http://schema-registry:8081',"
                             "'value.converter'='io.confluent.connect.avro.AvroConverter',"
                             "'value.converter.schemas.enable'='true',"
                             "'value.converter.schema.registry.url'= 'http://schema-registry:8081',"
                             "'pk.mode'='record_key', 'pk.fields'='TWEET_ID','Insert.mode'='upsert',"
                             "'auto.create'='true','auto.evolve'='true',"
                             "'table.name.format'='${topic}-01');")

    def query(self):
        q = self.client.query("SELECT * FROM BART_DELAYS EMIT CHANGES")
        for r in q:
            print(r)

    def run(self):
        self.create_stream()
        time.sleep(10)
        self.create_avro_stream()
        time.sleep(10)
        self.create_table()
        self.create_tweet_stream()
        time.sleep(10)
        self.create_tweet_avro_stream()
        time.sleep(10)
        self.create_delay_postgres_sink()
        self.create_tweet_postgres_sink()
        self.query()


def main():
    time.sleep(180)
    test = BARTKSQLDBTable()
    test.run()


if __name__ == '__main__':
    main()
