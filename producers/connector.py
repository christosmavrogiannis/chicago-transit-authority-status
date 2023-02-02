"""Configures a Kafka Connector for Postgres Station data by using Kafka Connect"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    # Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    logger.info("stations connector will be configured now")
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "topic.prefix": "connect-",
                "mode": "incrementing",
                "table.whitelist": "stations",
                "incrementing.column.name": "stop_id",
                "connection.url": "jdbc:postgresql://localhost:5434/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "poll.interval.ms": "60000"
            }
       }),
    )

    try:
        resp.raise_for_status()
        logging.debug("connector created successfully")
    except:
        logging.error(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    
if __name__ == "__main__":
    configure_connector()
