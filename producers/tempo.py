broker_properties = {
    "config": {
        "BROKER_URL" : "PLAINTEXT://localhost:9093"
    },
    "SCHEMA_REGISTRY_URL" : "http://localhost:8081"
}

print(broker_properties['config']['BROKER_URL'])