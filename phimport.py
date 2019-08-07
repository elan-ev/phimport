import influxdbconfig as cfg
import sys
import csv
import time
from influxdb import InfluxDBClient

client = InfluxDBClient(host=cfg.influx['host'], port=cfg.influx['port'], username=cfg.influx['user'], password=cfg.influx['password'], ssl=True, verify_ssl=True)

client.switch_database(cfg.influx['database'])

if len(sys.argv) != 2:
    print("you need to provide a valid csv file as parameter")
    sys.exit(1)

csvpath = sys.argv[1]
entries = []
added = 0
ignored = 0
nowtime=time.time_ns()

with open(csvpath, newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    headers = next(csvreader, None)
    for row in csvreader:
        if row[0] == "" or row[1] == "" or row[2] == "":
            print("illegal entry: ", end='')
            print(row)
            ignored+=1
        else:
            nowtime+=1
            added+=1
            entries.append({
                'measurement': cfg.influx['measurement'],
                'time': nowtime,
                'fields': {
                    'hours': float(row[2]),
                    },
                'tags': {
                    'organizationId': row[0],
                    },
                })

client.write_points(entries)
print("added %d entries" % added)
print("ignored %d entries" % ignored)
