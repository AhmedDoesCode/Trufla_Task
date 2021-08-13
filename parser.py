import pandas as pd
import xmltodict, json
from bs4 import BeautifulSoup
import datetime
import sys
import re

def parse_csv(customers_file,vehicles_file):
    print("Parsing CSV: " + customers_file + " - " +vehicles_file)

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