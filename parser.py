import pandas as pd
import xmltodict, json
from bs4 import BeautifulSoup
import datetime
import sys
import re

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
                vehicles.append({
                "id":vehicle['id'],
                "make":vehicle['make'],
                "vin_number":vehicle['vin_number'],
                "model_year": str(vehicle['model_year'])
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
        
    for index, customer in enumerate(output):
        with open('output/csv/'+str(ts)+'_'+customers_file_name[0]+'_'+vehicles_file_name[0]+'_'+str(index)+'.json', 'w', encoding='utf-8') as f:
            json.dump(output[index], f, ensure_ascii=True, indent=4)

    

def parse_xml(xml_file):
    x = re.search(r'[ \w-]+?(?=\.)', xml_file)
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
    print(type(data["Transaction"]["Customer"]["Units"]))
    if data["Transaction"]["Customer"]["Units"] is not None:
        if type(data["Transaction"]["Customer"]["Units"]["Auto"]["Vehicle"]) is not list:
            data["Transaction"]["Customer"]["Units"]["Auto"]["Vehicle"] = [data["Transaction"]["Customer"]["Units"]["Auto"]["Vehicle"]]

        
        
        for vehicle in data["Transaction"]["Customer"]["Units"]["Auto"]["Vehicle"]:
            print(vehicle)
            output["transaction"]["vehicles"].append({
                "id": vehicle["@id"],
                "make": vehicle["Make"],
                "vin_number": vehicle["VinNumber"],
                "model_year": vehicle["ModelYear"],
            })

    ct = datetime.datetime.now()
    ts = ct.timestamp()

    with open('output/xml/'+str(ts)+'_'+x[0]+'.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=True, indent=4)
    


if __name__ == '__main__':
    input_ext = sys.argv[1]
    if input_ext == "csv":
        customers_file = sys.argv[2]
        vehicles_file = sys.argv[3]
        parse_csv(customers_file, vehicles_file)
    elif input_ext == "xml":
        xml_file = sys.argv[2]
        parse_xml(xml_file)
    else:
        print("ERR: Wrong file format")