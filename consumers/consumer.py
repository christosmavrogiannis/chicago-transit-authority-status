"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers" : "PLAINTEXT://localhost:9092",
            "group.id": "first-python-consumer-group",
            "auto.offset.reset": "earliest" if offset_earliest else "latest"
        }

        if is_avro is True:
            schema_registry = CachedSchemaRegistryClient({"url": "http://localhost:8081" })
            self.consumer = AvroConsumer(self.broker_properties,  schema_registry=schema_registry)
        else:
            self.consumer = Consumer(self.broker_properties)

        # Configure the consumer and subscribe to the topics. 
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)
        


    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        
        # Poll Kafka for messages.
        # Make sure you return 1 when a message is processed, and 0 when no message is retrieved.
        message = self.consumer.poll(1.0) # timeout = 1.0
        if message is None:
            logger.debug("No message received")
            return 0
        try:
            self.message_handler(message)
            return 1
        except Exception as exception:
            logger.error(exception)
            return 0

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()