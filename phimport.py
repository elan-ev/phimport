import influxdbconfig as cfg
import sys
import csv
import os
import time
from influxdb import InfluxDBClient

os.environ['TZ'] = 'UTC'
time.tzset()
print('Current timezone is', time.tzname)

client = InfluxDBClient(host=cfg.influx['host'], port=cfg.influx['port'], username=cfg.influx['user'], password=cfg.influx['password'], ssl=True, verify_ssl=True)

client.switch_database(cfg.influx['database'])

if len(sys.argv) != 2:
    print("you need to provide a valid csv file as parameter")
    sys.exit(1)

csvpath = sys.argv[1]
entries = {}
added = 0
ignored = 0


with open(csvpath, newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    headers = next(csvreader, None)
    for row in csvreader:
        if row[0] == "" or row[1] == "" or row[2] == "" or row[3] == "":
            print("illegal entry: ", end='')
            print(row)
            ignored+=1
        else:
            eventid = row[0]
            eventdate = row[1]
            eventhours = float(row[2])
            eventorg = row[3]
            parsedeventdate = int(time.mktime(time.strptime(eventdate, '%Y-%m-%d')) * 1000000000)
            added+=1
            if (parsedeventdate, eventorg) in entries:
                entries[(parsedeventdate, eventorg)]['fields']['hours'] += eventhours
            else:
                entries[(parsedeventdate, eventorg)] = {
                    'measurement': cfg.influx['measurement'],
                    'time': parsedeventdate,
                    'fields': {
                        'hours': eventhours,
                        },
                    'tags': {
                        'organizationId': eventorg,
                        },
                    }
client.write_points([e for e in entries.values()])
print("added %d entries" % added)
print("ignored %d entries" % ignored)
