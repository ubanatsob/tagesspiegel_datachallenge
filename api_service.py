import json
import os
import sys
import requests
import pytz
from statistics import mean
from datetime import datetime
from google.cloud import bigquery

def extract_data(start_date, end_date, location, api_key):

    url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{location}/{start_date}/{end_date}"
    params = {'key': api_key}
    res = requests.get(url, params=params)

    if res.status_code == 200:
        data = res.json()
        return data
    
    if res.status_code == 429:
        print("Error: ",res.text)

        # Wenn die Anzahl der Maximalen Requests erreicht wurde, wird ein Beispiel-Datensatz fuer Demonstrationszwecke genommen
        # Lesen aus der Datei
        with open('./test_data.json', 'r') as json_file:
            data = json.load(json_file)
       
 
        #json_object = json.loads(data)
        return data

    else:
        print("Der Request ist fehlgeschlagen: ", res.text)

def transform_data(e):
    average_temp = []
    cloudcover = []
    conditions = []

    resolved_address = e['resolvedAddress']
    start_date = e["days"][0]["datetime"]
    query_cost = e['queryCost']

    for x in e["days"]:
        for y in x["hours"]:
            average_temp.append(y["temp"])
            cloudcover.append(y["cloudcover"])
            conditions.append(y["icon"])
    
    fog = is_foggy(conditions)
    drizzle = drizzle_days(conditions)
    max_cloudcover = max(cloudcover)
    average_temp = round(fahrenheit_to_celsius(mean(average_temp)), 2)
    runtime_timestamp = current_timestamp_timezone(e["timezone"])
    

    json_data = {
        "resolved_address": resolved_address,
        "start_date": start_date,
        "query_cost": query_cost,
        "average_temp": average_temp,
        "max_cloudcover":max_cloudcover,
        "fog": fog,
        "days_with_drizzle": drizzle,
        "runtime_timestamp": runtime_timestamp
    }
    return json_data
    
def is_foggy(conditions):
    if "fog" in conditions:
        return True
    else: 
        return False

def drizzle_days(conditions):
    days=0
    for x in conditions:
        if "drizzle" in conditions:
            days += 1
    return days

def fahrenheit_to_celsius(fahrenheit):
    return (fahrenheit - 32) * 5/9

def current_timestamp_timezone(timezone):
    timezone = pytz.timezone(timezone) 
    current_timestamp_timezone = datetime.now(timezone)
    current_timestamp_timezone = current_timestamp_timezone.strftime("%Y-%m-%d %H:%M:%S")
    return current_timestamp_timezone

def load_data(t,google_application_credentials_path, big_query_target_id):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials_path

    project, dataset, table = big_query_target_id.split(".")

    client = bigquery.Client(project=project)

    schema = [
        bigquery.SchemaField("resolved_address", "STRING"),
        bigquery.SchemaField("start_date", "DATE"),
        bigquery.SchemaField("query_cost", "INTEGER"),
        bigquery.SchemaField("average_temp", "FLOAT"),
        bigquery.SchemaField("max_cloudcover", "FLOAT"),
        bigquery.SchemaField("fog", "BOOLEAN"),
        bigquery.SchemaField("days_with_drizzle", "INTEGER"),
        bigquery.SchemaField("runtime_timestamp", "TIMESTAMP"),
    ]

    table_ref = client.dataset(dataset).table(table)
    table = bigquery.Table(table_ref, schema=schema)

    rows_to_insert = [(t["resolved_address"], t["start_date"], t["query_cost"],
                    t["average_temp"], t["max_cloudcover"], t["fog"],
                    t["days_with_drizzle"], t["runtime_timestamp"])]
    errors = client.insert_rows(table, rows_to_insert)

    if errors:
        print(f"Fehler beim laden der Daten: {errors}")
    else:
        print("Daten wurden erfolgreich geladen")



if __name__ == "__main__":

    if len(sys.argv) != 7:
        print("Usage: python api_service.py START_DATE END_DATE LOCATION API_KEY GOOGLE_APPLICATION_CREDENTIALS_PATH BIG_QUERY_TARGET_ID")
        sys.exit(1)
    
    start_date, end_date, location, api_key, google_application_credentials_path, big_query_target_id = sys.argv[1:]
    
    e = extract_data(start_date, end_date, location, api_key)
    t = transform_data(e)
    l = load_data(t, google_application_credentials_path, big_query_target_id)




