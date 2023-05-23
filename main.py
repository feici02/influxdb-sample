import argparse
import configparser

import influxdb_client
import time
from influxdb_client import Point
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
    parser.add_argument("-c", "--count", help="count, 0 for non-stop", type=int)
    return parser.parse_args()


def write_influxdb(write_api, org, bucket, i):
    point = (
        Point("testdata")
        .tag("tagname1", "tagvalue1")
        .field("field1", i)
        .field("field2", "hello" + str(i))
        .field("field3", True if i % 2 == 0 else False)
    )
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
            write_influxdb(write_api, org, bucket, i)
            i += 1
            time.sleep(1)  # separate points by 1 second


if __name__ == '__main__':
    main()
