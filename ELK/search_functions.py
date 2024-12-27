from elasticsearch import Elasticsearch
from dotenv import load_dotenv
load_dotenv()
import os

# Initialize Elasticsearch client
es = Elasticsearch(f"http://{os.getenv('ELASTICSEARCH_HOST')}:{os.getenv('ELASTICSEARCH_PORT')}")

def get_data_results(response):
    records = [hit['_source'] for hit in response['hits']['hits']]
    return records
#Search Alerts by Severity
def search_alerts_by_severity(severity):
    query = {
        "query": {
            "term": {
                "severity": severity
            }
        }
    }
    results = es.search(index="alerts_index", body=query)
    return get_data_results(results)

#Find Busy Roads at Specific Times
def find_busy_roads(start_time, end_time):
    query = {
        "query": {
            "range": {
                "hour": {
                    "gte": start_time,
                    "lte": end_time
                }
            }
        },
        "sort": [
            { "vehicle_count": { "order": "desc" } }
        ]
    }
    results = es.search(index="traffic_df", body=query)
    return get_data_results(results)
#Analyze Vehicle Metrics by Type
def get_metrics_by_vehicle_type(vehicle_type):
    query = {
        "query": {
            "term": {
                "vehicle_type": vehicle_type
            }
        }
    }
    results = es.search(index="metrics_index", body=query)
    return get_data_results(results)
#Top 5 Most Frequent Alert Types
def get_top_alert_types():
    query = {
        "size":5,
        "sort": [
            {"alert_count": {"order": "desc"}}
        ]
    }
    results = es.search(index="alerts_index", body=query)
    return get_data_results(results)

#Predictive Traffic Alerts
def find_potential_traffic_jams(min_speed=20, min_vehicle_count=1):
    query = {
        "query": {
            "bool": {
                "filter": [
                    { "range": { "avg_speed": { "gte": min_speed } } },
                    { "range": { "vehicle_count": { "gte": min_vehicle_count } } }
                ]
            }
        }
    }
    results = es.search(index="traffic_df", body=query)
    return get_data_results(results)

#Alerts Prioritization
def get_high_priority_alerts():
    query = {
        "query": {
            "term": {
                "severity": "High"
            }
        },
        "sort": [
            { "timestamp": { "order": "desc" } }
        ]
    }
    results = es.search(index="alerts_index", body=query)
    return get_data_results(results)

def display_records(index_name):
    query = {
        "query": {
            "match_all": {}
        },
    }

    response = es.search(index=index_name, body=query)
    records = [hit['_source'] for hit in response['hits']['hits']]

    print(f"Displaying {len(records)} records from index '{index_name}':")
    for record in records:
        print(record)

    return records

