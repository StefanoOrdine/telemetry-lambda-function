#!/usr/bin/python
import sys
import logging
import psycopg2
import json
import os

def make_conn():
    db_name = os.environ['DATABASE_NAME']
    db_user = os.environ['DATABASE_USER']
    db_host = os.environ['DATABASE_HOST']
    db_port = os.environ['DATABASE_PORT']
    db_pass = os.environ['DATABASE_PASSWORD']
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' port='%s' password='%s' sslmode='prefer'" % (db_name, db_user, db_host, db_port, db_pass))
    return conn

def persist_telemetry(conn, telemetry):
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO telemetries (timestamp, pressure, temperature, humidity, light, roll, pitch, battery_voltage)
    VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s)
    """, telemetry)
    conn.commit()
    return cursor.statusmessage

def extract_telemetry(event):
    body = json.loads(event['body'])
    payload = json.loads(body['payload'])
    _temperature, _altitude, pressure, si_temperature, humidity, \
    _light_blue, _light_red, light, roll, pitch, battery_voltage = payload
    return (pressure, si_temperature, humidity, light, roll, pitch, battery_voltage)

def lambda_handler(event, context):
    telemetry = extract_telemetry(event)
    conn = make_conn()
    statusmessage = persist_telemetry(conn, telemetry)
    conn.close()
    result = "%s %s" % (statusmessage, telemetry)
    print(result)
    return result
