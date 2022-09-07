import json
from tweepy import StreamingClient, StreamRule
from kafka import KafkaProducer
from config import bearer_token as bearer_token_import
from time import sleep


class TweetPrinter(StreamingClient):
    def __init__(self, bearer_token, **kwargs):
        super().__init__(bearer_token, **kwargs)
        self.bearer_token = bearer_token_import
        self.kafka_topic = "BART_TWEETS"
        self.kafka_producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 11, 5),
                                            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                            key_serializer=str.encode)

    def on_tweet(self, tweet):
        sleep(60)
        message = {
                "TWEET_ID": tweet.id,
                "CREATED_DATE": tweet.created_at,
                "TWEET_TEXT": tweet.text
            }
        self.kafka_producer.send(self.kafka_topic, message, str(tweet.id))
        self.kafka_producer.flush()

    def create_rules(self):
        rule_ids = []
        result = self.get_rules()
        for rule in result.data:
            print(f"rule marked to delete: {rule.id} - {rule.value}")
            rule_ids.append(rule.id)

        if len(rule_ids) > 0:
            self.delete_rules(rule_ids)
            print("deleted rules")
        else:
            print("no rules to delete")

        rule1 = StreamRule(value="@SFBART")
        rule2 = StreamRule(value="#SFBART")
        rule3 = StreamRule(value="BART Delay")
        rule4 = StreamRule(value="SFBART Delay")
        self.add_rules([rule1, rule2, rule3, rule4])
        print("created rules")

    def run(self):
        self.create_rules()
        self.filter()


def main():
    tweet_stream = TweetPrinter(bearer_token=bearer_token_import)
    tweet_stream.run()


if __name__ == "__main__":
    main()
