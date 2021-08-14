import urllib.request as request
import pandas as pd
import xmltodict, json
import datetime
import sys
import re
import urllib
from pymongo import MongoClient
import pymongo

def get_database():
    CONNECTION_STRING = "mongodb+srv://trufla_admin:" + urllib.parse.quote("P@ssw0rd") + "@trufla.if4c2.mongodb.net/trufla?retryWrites=true&w=majority"
    
    trufla_ = MongoClient(CONNECTION_STRING)
    return trufla_['trufla']  
    
    

def parse_csv(customers_file,vehicles_file):
    #print("Parsing CSV: " + customers_file + " - " +vehicles_file)
    customers_file_DF = pd.read_csv(customers_file)
    vehicles_file_DF = pd.read_csv(vehicles_file)
    customers_file_name = re.search(r'[ \w-]+?(?=\.)', customers_file)
    vehicles_file_name = re.search(r'[ \w-]+?(?=\.)', vehicles_file)
    output = []
    for c_index,customer in customers_file_DF.iterrows():
        vehicles = []
        for v_index,vehicle in vehicles_file_DF.iterrows():
            if vehicle['owner_id'] == customer['id']:
                enriched_data = enrich(vehicle['vin_number'],str(vehicle['model_year']))
                if enriched_data == 0:
                    vehicles.append({
                    "id":vehicle['id'],
                    "make":vehicle['make'],
                    "vin_number":vehicle['vin_number'],
                    "model_year": str(vehicle['model_year']),
                    },)
                else:
                    vehicles.append({
                    "id":vehicle['id'],
                    "make":vehicle['make'],
                    "vin_number":vehicle['vin_number'],
                    "model_year": str(vehicle['model_year']),
                    "model": enriched_data["Results"][0]["Model"],
                    "manufacturer": enriched_data["Results"][0]["Manufacturer"],
                    "plant_country": enriched_data["Results"][0]["PlantCountry"],
                    "vehicle_type": enriched_data["Results"][0]["VehicleType"]
                    },)

        output.append({
            "customer_file_name": customers_file,
            "vehicle_file_name": vehicles_file,
            "transaction":{
                "date":customer['date'],
                "customer":{
                    "id":customer['id'],
                    "name":customer['name'],
                    "address":customer['address'],
                    "phone":customer['phone']
                },
                "vehicles":vehicles
            }
        })
    ct = datetime.datetime.now()
    ts = ct.timestamp()
    
    csv_collection.insert_many(output)
    for index, customer in enumerate(output):

        with open('output/csv/'+str(ts)+'_'+customers_file_name[0]+'_'+vehicles_file_name[0]+'_'+str(index)+'_enriched.json', 'w', encoding='utf-8') as f:
            json.dump(output[index], f, ensure_ascii=True, indent=4)

    

def parse_xml(xml_file):
    x = re.search(r'[ \w-]+?(?=\.)', xml_file)
    ct = datetime.datetime.now()
    ts = ct.timestamp()
    output = {
        "file_name": xml_file,
        "transaction":{
            "date":"",
            "customer":{
                "id":"",
                "name":"",
                "address":"",
                "phone":""
            },
            "vehicles":[]
        }
    }
    output["file_name"] = xml_file
    with open(xml_file, 'r') as f:
        data = xmltodict.parse(f.read())
    
    data = data["Insurance"]
    output["transaction"]["date"] = data["Transaction"]["Date"]
    output["transaction"]["customer"]["id"] = data["Transaction"]["Customer"]["@id"]
    output["transaction"]["customer"]["name"] = data["Transaction"]["Customer"]["Name"]
    output["transaction"]["customer"]["address"] = data["Transaction"]["Customer"]["Address"]
    output["transaction"]["customer"]["phone"] = data["Transaction"]["Customer"]["Phone"]
    if data["Transaction"]["Customer"]["Units"] is not None:
        if type(data["Transaction"]["Customer"]["Units"]["Auto"]["Vehicle"]) is not list:
            data["Transaction"]["Customer"]["Units"]["Auto"]["Vehicle"] = [data["Transaction"]["Customer"]["Units"]["Auto"]["Vehicle"]]

        
        
        for vehicle in data["Transaction"]["Customer"]["Units"]["Auto"]["Vehicle"]:
            enriched_data = enrich(vehicle["VinNumber"],vehicle["ModelYear"])
            if enriched_data == 0:
                output["transaction"]["vehicles"].append({
                    "id": vehicle["@id"],
                    "make": vehicle["Make"],
                    "vin_number": vehicle["VinNumber"],
                    "model_year": vehicle["ModelYear"],
                })
                xml_collection.insert_one(output)
                with open('output/xml/'+str(ts)+'_'+x[0]+'.json', 'w', encoding='utf-8') as f:
                    json.dump(output, f, ensure_ascii=True, indent=4)
                return
            else:
                output["transaction"]["vehicles"].append({
                    "id": vehicle["@id"],
                    "make": vehicle["Make"],
                    "vin_number": vehicle["VinNumber"],
                    "model_year": vehicle["ModelYear"],
                    "model": enriched_data["Results"][0]["Model"],
                    "manufacturer": enriched_data["Results"][0]["Manufacturer"],
                    "plant_country": enriched_data["Results"][0]["PlantCountry"],
                    "vehicle_type": enriched_data["Results"][0]["VehicleType"]
                })
                xml_collection.insert_one(output)
                with open('output/xml/'+str(ts)+'_'+x[0]+'_enriched.json', 'w', encoding='utf-8') as f:
                    json.dump(output, f, ensure_ascii=True, indent=4)
                return
    

def enrich(vin,model):
    with request.urlopen('https://vpic.nhtsa.dot.gov/api/vehicles/decodevinvalues/'+vin+'?format=json&modelyear='+model) as response:
        if response.getcode() == 200:
            source = response.read()
            data = json.loads(source)
            return data
        else:
            print('An error occurred while attempting to retrieve data from the API.')
            return 0

if __name__ == '__main__':
    input_ext = sys.argv[1]
    # Get the database
    trufla_db = get_database()
    xml_collection = trufla_db["xml"]
    csv_collection = trufla_db["csv"]
    if input_ext == "csv":
        customers_file = sys.argv[2]
        vehicles_file = sys.argv[3]
        parse_csv(customers_file, vehicles_file)
    elif input_ext == "xml":
        xml_file = sys.argv[2]
        parse_xml(xml_file)
    else:
        print("ERR: Wrong file format")