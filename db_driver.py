import csv
import datetime
import io
import zipfile

import requests


def refreshdb(c):
    lastUp = int(datetime.date.today().strftime("%Y%m%d"))

    gtfsZip = requests.get('http://webapps.thebus.org/transitdata/Production/google_transit.zip')
    gtfs = zipfile.ZipFile(io.BytesIO(gtfsZip.content))

    # Routes logic
    with io.StringIO(gtfs.read('routes.txt').decode('ASCII')) as routesCSV:
        reader = csv.DictReader(routesCSV)
        c.execute('DROP TABLE IF EXISTS routes')
        c.execute("""
        CREATE TABLE routes
        (
            id int PRIMARY KEY NOT NULL,
            desc text,
            sign text
        )""")

        c.execute("CREATE UNIQUE INDEX routes_id_uindex ON routes (id)")
        c.execute("CREATE UNIQUE INDEX routes_sign_uindex ON routes (sign)")
        for row in reader:
            c.execute("INSERT INTO routes VALUES (?,?,?)",
                      (row['route_id'], row['route_long_name'], row['route_short_name']))
        routesCSV.close()

    # Stops Logic
    with io.StringIO(gtfs.read('stops.txt').decode('ASCII')) as stopsCSV:
        reader = csv.DictReader(stopsCSV)
        c.execute('DROP TABLE IF EXISTS stops')
        c.execute("""
                CREATE TABLE stops
            (
                id int PRIMARY KEY NOT NULL,
                lat real,
                lon real,
                name text
            )""")
        c.execute("CREATE UNIQUE INDEX stops_id_uindex ON stops (id)")
        for row in reader:
            c.execute(
                'INSERT or IGNORE INTO "stops" ("id", "lat", "lon", "name") VALUES (?,?,?,?)',
                (row['stop_code'], row['stop_lat'], row['stop_lon'], row['stop_name'].title()))

        stopsCSV.close()

    # Calendar Logic
    with io.StringIO(gtfs.read('calendar.txt').decode('ASCII')) as calCSV:
        reader = csv.DictReader(calCSV)
        c.execute('DROP TABLE IF EXISTS cal')
        c.execute("""
            CREATE TABLE cal
            (
                service_id int PRIMARY KEY,
                days text(7)
            )""")
        c.execute("CREATE UNIQUE INDEX cal_service_id_uindex ON cal (service_id)")
        for row in reader:
            if int(row['end_date']) < lastUp:
                c.execute('INSERT into cal VALUES (?,?)',
                          (row['service_id'], row['operating_days']))

        calCSV.close()

    # Trips Logic
    with io.StringIO(gtfs.read('trips.txt').decode('ASCII')) as tripCSV:
        reader = csv.DictReader(tripCSV)
        c.execute('DROP TABLE IF EXISTS trips')
        c.execute("""  
                  CREATE TABLE trips
            (
                id int PRIMARY KEY,
                service_id int NOT NULL,
                route_id int,
                direction boolean,
                shape_id text,
                sign text
            )
          """)
        c.execute("CREATE UNIQUE INDEX trips_id_uindex ON trips (id)")
        for row in reader:
            c.execute('INSERT into trips VALUES (?,?,?,?,?,?)',
                      (row["trip_id"], row["service_id"],
                       row["route_id"], row["direction_id"],
                       row["shape_id"], row["trip_headsign"].title()))

        tripCSV.close()

    # Times Logic
    with io.StringIO(gtfs.read('stop_times.txt').decode('ASCII')) as timeCSV:
        reader = csv.DictReader(timeCSV)
        c.execute('DROP TABLE IF EXISTS times')
        c.execute("""  
            CREATE TABLE times
            (
                trip_id int,
                arrival_time time,
                departure_time time,
                stop_id int,
                stop_sequence int,
                shape_dist_traveled float DEFAULT 0
            )
          """)
        for row in reader:
            c.execute('INSERT into times VALUES (?,?,?,?,?,?)',
                      (row["trip_id"], row["arrival_time"],
                       row["departure_time"], row["stop_id"],
                       row["stop_sequence"], row["shape_dist_traveled"]))
        timeCSV.close()
