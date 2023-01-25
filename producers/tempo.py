broker_properties = {
    "config": {
        "BROKER_URL" : "PLAINTEXT://localhost:9093"
    },
    "SCHEMA_REGISTRY_URL" : "http://localhost:8081"
}

print(broker_properties['config']['BROKER_URL'])



schema_registry = CachedSchemaRegistryClient({"url": self.schema_registry_url })
producer = AvroProducer(self.config, schema_registry=schema_registry)
broker_properties['config']