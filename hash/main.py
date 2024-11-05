from elasticsearch import Elasticsearch, helpers, exceptions
import pandas as pd

es = Elasticsearch("http://localhost:8282", verify_certs=False)

try:
    if es.ping():
        print("Connected to Elasticsearch.")
    else:
        print("Failed to connect to Elasticsearch.")
except exceptions.ConnectionError:
    print("Error: Could not connect to Elasticsearch.")
    exit()

name = input("Enter your name: ")
phone = input("Enter your phone number (last four digits): ")

v_nameCollection = f'hash_{name.lower()}'
v_phoneCollection = f'hash_{phone[-4:]}'

def createCollection(p_collection_name):
    try:
        if not es.indices.exists(index=p_collection_name):
            es.indices.create(index=p_collection_name)
            print(f"Collection '{p_collection_name}' created.")
        else:
            print(f"Collection '{p_collection_name}' already exists.")
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except exceptions.TransportError as e:
        print(f"Elasticsearch error: {e}")

def indexData(p_collection_name, p_exclude_column):
    try:
        data = pd.read_csv("employee_data\Employee Sample Data.csv", encoding='ISO-8859-1').drop(columns=[p_exclude_column])
        actions = [
            {
                "_index": p_collection_name,
                "_id": row["Employee ID"],
                "_source": row.to_dict()
            }
            for _, row in data.iterrows()
        ]
        helpers.bulk(es, actions)
        print(f"Data indexed into collection '{p_collection_name}' excluding '{p_exclude_column}' column.")
    except FileNotFoundError:
        print("CSV file not found.")
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    try:
        query = {
            "query": {
                "match": {
                    p_column_name: p_column_value
                }
            }
        }
        res = es.search(index=p_collection_name, body=query)
        print(f"Search results for {p_column_name}={p_column_value} in '{p_collection_name}':")
        return res['hits']['hits']
    except exceptions.NotFoundError:
        print(f"Collection '{p_collection_name}' not found.")
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")

def getEmpCount(p_collection_name):
    try:
        res = es.count(index=p_collection_name)
        print(f"Employee count in '{p_collection_name}': {res['count']}")
        return res['count']
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")

def delEmpById(p_collection_name, p_employee_id):
    try:
        res = es.delete(index=p_collection_name, id=p_employee_id, ignore=[404])
        print(f"Deleted employee ID {p_employee_id} from '{p_collection_name}'.")
        return res
    except exceptions.NotFoundError:
        print(f"Employee ID {p_employee_id} not found.")
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")

def getDepFacet(p_collection_name):
    try:
        query = {
            "size": 0,
            "aggs": {
                "departments": {
                    "terms": {
                        "field": "Department.keyword"
                    }
                }
            }
        }
        res = es.search(index=p_collection_name, body=query)
        print("Employee count grouped by department:")
        return res['aggregations']['departments']['buckets']
    except exceptions.ConnectionError as e:
        print(f"Connection error: {e}")

createCollection(v_nameCollection)
createCollection(v_phoneCollection)
getEmpCount(v_nameCollection)
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')
getEmpCount(v_nameCollection)
delEmpById(v_nameCollection, 'E02003')
getEmpCount(v_nameCollection)
print(searchByColumn(v_nameCollection, 'Department', 'IT'))
print(searchByColumn(v_nameCollection, 'Gender', 'Male'))
print(searchByColumn(v_phoneCollection, 'Department', 'IT'))
print(getDepFacet(v_nameCollection))
print(getDepFacet(v_phoneCollection))
