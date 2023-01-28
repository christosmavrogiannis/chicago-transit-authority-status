"""Producer base-class providing common utilites and functionality"""
import logging
import time
from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances, kind of static variable
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        self.broker_properties = {
            "config": {
                "bootstrap.servers" : "PLAINTEXT://localhost:9093"
            },
            "SCHEMA_REGISTRY_URL" : "http://localhost:8081"    
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            
        # Configure the AvroProducer
        schema_registry = CachedSchemaRegistryClient({"url": self.broker_properties['SCHEMA_REGISTRY_URL'] })
        self.producer = AvroProducer(self.broker_properties['config'], schema_registry=schema_registry)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.#
        logger.info("A new topic will be created.")
        topic = NewTopic(
            topic = self.topic_name, 
            num_partitions = self.num_partitions, 
            replication_factor = self.num_replicas)
        client = AdminClient(self.broker_properties['config'])
        result = client.create_topics([topic])
        for topic, f in result.items():
            try:
                f.result()  # The result itself is None
                logger.info("Topic '{}' created".format(topic))
                Producer.existing_topics.add(self.topic_name)
            except Exception as exception:
                logger.error("Failed to create topic {}: {}".format(topic, exception))

    def delete_topic(self):
        logger.info("The topic '{}' will now be deleted.".format(self.topic_name))
        self.client.delete_topics([self.topic_name])

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # A synchronous operation - (from Doc) Wait for all messages in the Producer queue to be delivered
        self.producer.flush()
        self.delete_topic()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
