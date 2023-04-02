"""Defines trends calculations for stations"""
import logging
import faust
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


@dataclass
# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

def transform_station_info(station):
    logger.info("--------")
    message= f"Station id : '{station.station_id}' - station name: '{station.station_name}'"
    logger.info(message)
    
    line = ''
    if station.red:
        line = 'red'
    if station.blue:
        line = 'blue'
    if station.green:
        line = 'green'
  
    transformed_station = TransformedStation(station.station_id, station.station_name, station.order, line)
  
    return transformed_station

# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
# places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9093", store="memory://")
# Define the input Kafka Topic to which Kafka Connect outputs
topic = app.topic("org.chicago.cta.postgres.stations", value_type=Station)
# Define the output Kafka Topic
out_topic = app.topic("org.chicago.cta.stations.table.v1", partitions=1,  value_type=TransformedStation)

table = app.Table(
    "transformed_stations",
    default=int)


# Transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`

@app.agent(topic)
async def station_event(station_stream):
    async for station in station_stream:
        logger.info(type(station))
        table[station.station_id] = transform_station_info(station)


if __name__ == "__main__":
    app.main()
