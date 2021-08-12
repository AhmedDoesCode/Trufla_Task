import pandas as pd
import xmltodict, json
from bs4 import BeautifulSoup
import sys

def parse_csv(customers_file,vehicles_file):
    print("Parsing CSV: " + customers_file + " - " +vehicles_file)

def parse_xml(xml_file):
    print("Parsing XML: " + xml_file)
    


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