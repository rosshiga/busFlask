import sqlite3
import io
import requests
import zipfile
import csv

sqlite_file = 'database.sqlite'

gtfsZip = requests.get('http://webapps.thebus.org/transitdata/Production/google_transit.zip')
gtfs = zipfile.ZipFile(io.BytesIO(gtfsZip.content))

routesCSV = io.StringIO(gtfs.read('routes.txt').decode('ASCII'))
reader = csv.DictReader(routesCSV)
for row in reader:
    print(row['route_long_name'])


# Connecting to the database file
conn = sqlite3.connect(sqlite_file)
c = conn.cursor()

# Creating a second table with 1 column and set it as PRIMARY KEY
# note that PRIMARY KEY column must consist of unique values!
#c.execute('CREATE TABLE {tn} ({nf} {ft} PRIMARY KEY)'\
#        .format(tn=table_name2, nf=new_field, ft=field_type))

# Committing changes and closing the connection to the database file
conn.commit()
conn.close()