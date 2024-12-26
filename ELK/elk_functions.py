from elasticsearch import Elasticsearch

# Initialize Elasticsearch client
es = Elasticsearch(["http://127.0.0.1:9200"])
print(es.info())

def create_metrics_index():
    """
    Create an index for metrics data.
    """
    mappings = {
        "mappings": {
            "properties": {
                "vehicle_type": { "type": "keyword" },
                "avg_speed": { "type": "float" },
                "avg_trip_duration": { "type": "float" },
                "total_trips": { "type": "integer" }
            }
        }
    }
    es.indices.create(index="metrics_index", body=mappings, ignore=400)
    print("Metrics index created!")

def create_traffic_index():
    """
    Create an index for traffic data.
    """
    mappings = {
        "mappings": {
            "properties": {
                "hour": { "type": "date", "format": "yyyy-MM-dd'T'HH:mm:ssZ" },
                "road": {
                    "properties": {
                        "district": { "type": "keyword" },
                        "city": { "type": "keyword" }
                    }
                },
                "vehicle_count": { "type": "integer" },
                "avg_speed": { "type": "float" }
            }
        }
    }
    es.indices.create(index="traffic_index", body=mappings, ignore=400)
    print("Traffic index created!")

def create_alerts_index():
    """
    Create an index for alerts data.
    """
    mappings = {
        "mappings": {
            "properties": {
                "vehicle_id": { "type": "keyword" },
                "timestamp": { "type": "date", "format": "yyyy-MM-dd'T'HH:mm:ssZ" },
                "type": { "type": "keyword" },
                "severity": { "type": "keyword" },
                "description": { "type": "text" }
            }
        }
    }
    es.indices.create(index="alerts_index", body=mappings, ignore=400)
    print("Alerts index created!")

def create_summary_index():
   try:
       mappings = {
           "mappings": {
               "properties": {
                   "type": {"type": "keyword"},
                   "severity": {"type": "keyword"},
                   "alert_count": {"type": "integer"},
                   "descriptions": {"type": "text"}
               }
           }
       }
       es.indices.create(index="summary_index", body=mappings, ignore=400)
       print("Summary index created!")
   except Exception as e:
       print("Error: ",str(e))

def fetch_elk_indexs():
    indices = es.indices.get_alias("*")
    print(indices.keys())

def remove_index_from_elk(index_name):
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Index '{index_name}' deleted successfully.")
    else:
        print(f"Index '{index_name}' does not exist.")

