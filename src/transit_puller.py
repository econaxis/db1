from datetime import datetime
import requests
import my_pb2
import sys

sys.path.append("../target/debug/")

from libpythonlib import *

prev_cache = {}
prev_timestamps = {}

def main() -> None:
    print("Starting")

    response = requests.get("https://gtfs.translink.ca/v2/gtfsposition?apikey=XkJgz46eM82zRr0B3GD7").content
    msg = my_pb2.FeedMessage()
    msg.ParseFromString(response)

    to_insert = []

    now = datetime.now().timestamp()


    for entity in msg.entity:
        vehicle = entity.vehicle
        timestamp = vehicle.timestamp

        if vehicle.position.latitude == 0:
            continue

        if prev_cache.get(vehicle.vehicle.id, 0) != timestamp:
            prev_cache[vehicle.vehicle.id] = timestamp

            while timestamp in prev_timestamps:
                timestamp += 1

            prev_timestamps[timestamp] = timestamp

            try:
                store(trip_id=int(vehicle.trip.trip_id),
                      start_date=vehicle.trip.start_date,
                      route_id=vehicle.trip.route_id,
                      direction_id=bool(vehicle.trip.direction_id),
                      latitude=vehicle.position.latitude, longitude=vehicle.position.longitude,
                      current_stop_sequence=vehicle.current_stop_sequence, timestamp=timestamp,
                      stop_id=int(vehicle.stop_id), vehicle_id=int(vehicle.vehicle.id))
            except Exception as e:
                print(entity)
                raise e


import time
while True:
    main()
    time.sleep(5)
