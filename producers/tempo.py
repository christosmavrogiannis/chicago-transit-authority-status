broker_properties = {
    "config": {
        "BROKER_URL" : "PLAINTEXT://localhost:9093"
    },
    "SCHEMA_REGISTRY_URL" : "http://localhost:8081"
}

print(broker_properties['config']['BROKER_URL'])

from pathlib import Path
import json
with open(f"{Path(__file__).parents[0]}/models/schemas/weather_key.json") as f:
    key_schema = json.load(f)

print(type(key_schema))

key_schema_in_string=json.dumps(key_schema)
print(type(key_schema_in_string))