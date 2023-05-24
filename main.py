import argparse
import configparser
import time

import influxdb_client
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def parse_config():
    config = configparser.ConfigParser()
    config.read("config.ini")

    return config.get("influxdb", "url"), \
        config.get("influxdb", "org"), \
        config.get("influxdb", "bucket"), \
        config.get("influxdb", "token")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--write", help="write", action="store_true")
    parser.add_argument("-c", "--count", help="count, 0 for non-stop", type=int, required=True)
    parser.add_argument("-p", "--precision", help="time precision", choices=["s", "ms", "us", "ns"], required=True)
    return parser.parse_args()


def write_influxdb(write_api, org, bucket, i, precision):
    point = (
        Point("xwang2")
        .tag("tagname1", "tagvalue1")
        .tag("tagname2", 2.0)
        .tag("tagname3", 3)
        .tag("tagname4", True)
        .field("field1", i)
        .field("field2", "hello" + str(i))
        .field("field3", True if i % 2 == 0 else False)
        .field("field4", float(i))
    )

    if precision == "s":
        point.time(int(time.time_ns() / 1000000000), write_precision=WritePrecision.S)
    elif precision == "ms":
        point.time(int(time.time_ns() / 1000000), write_precision=WritePrecision.MS)
    elif precision == "us":
        pass
        point.time(int(time.time_ns() / 1000), write_precision=WritePrecision.US)
    elif precision == "ns":
        point.time(int(time.time_ns()), write_precision=WritePrecision.NS)

    write_api.write(org=org, bucket=bucket, record=point)


def main():
    url, org, bucket, token = parse_config()
    args = parse_args()
    client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

    if args.write:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        i = 0
        while True:
            if args.count and i == args.count:
                break

            print(f"write {i}")
            write_influxdb(write_api, org, bucket, i, args.precision)
            i += 1
            time.sleep(1)  # separate points by 1 second


if __name__ == '__main__':
    main()
