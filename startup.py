import sqlite3
import io
import requests
import zipfile
import csv

sqlite_file = 'database.sqlite'
# Connecting to the database file
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

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
        c.execute("INSERT INTO routes VALUES ({id},'{desc}','{sign}')" \
                  .format(id=row['route_id'], desc=row['route_long_name'], sign=row['route_short_name']))
    routesCSV.close()
    conn.commit()

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
            'INSERT or IGNORE INTO "stops" ("id", "lat", "lon", "name") VALUES ({id},{lat},{lon},"{name}")' \
            .format(id=row['stop_code'], lat=row['stop_lat'], lon=row['stop_lon'], name=row['stop_name'].title()))
    conn.commit()

# Creating a second table with 1 column and set it as PRIMARY KEY
# note that PRIMARY KEY column must consist of unique values!
# c.execute('CREATE TABLE {tn} ({nf} {ft} PRIMARY KEY)'\
#        .format(tn=table_name2, nf=new_field, ft=field_type))

# Committing changes and closing the connection to the database file
conn.commit()
conn.close()
