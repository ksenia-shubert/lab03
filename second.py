#KSENIA_SHUBERT_305

import requests
from bs4 import BeautifulSoup
import csv
import re
import os

link = "https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm"

def makeFolder():
    # folder for saving
    folder = "data"

    # create folder if it didnt exists
    if not os.path.exists(folder):
        os.makedirs(folder)
    else:
        # clear folder if it exists
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

def find_dataset(text):
    pattern = r'\b\w+\.txt\b'
    matches = re.findall(pattern, text)
    return matches


online = requests.get(link)
soup = BeautifulSoup(online.content, "html.parser")
find = soup.findAll("li", {"class": "MsoNormal"})
i = 0

cities = []
regions = []
realRegions = []

city_list = soup.findAll("li", {"class": "MsoNormal"})
for item in city_list:
    city = item.text.partition('(')[0].strip()
    cities.append(city)

    region = item.find_previous("p").text.strip().replace("\n", " ")
    if region:
        regions.append(region)

    region_span = item.find_previous("span", style="mso-bidi-font-size:10.0pt;font-family:Arial")
    if not region_span:
        region_span = item.find_previous("span", style=re.compile(r".*color:maroon.*"))
    region = region_span.text if region_span else ""
    realRegions.append(region)

data = []

set_south_america_region = False  

for item in find:
    result = find_dataset(item.text)
    osn = ''.join(result)
    dataset = osn[:len(osn) - 4]

    countries = regions[i]

    region_span = item.find("span", style="mso-bidi-font-size:10.0pt;font-family:Arial")
    if not region_span:
        region_span = item.find("span", style=re.compile(r".*color:maroon.*"))
    region = realRegions[i].replace("\n", " ").replace("\r", "")
    country_spans = item.findAll("span", style=re.compile(r".*color:maroon.*"))
    for span in country_spans:
        country = re.sub(r'[\[\]\r\n]', '', span.text).strip().replace("\n", " ").replace("\r", "")
        if country:
            countries += ", " + country

    
    if region == "":
        region = realRegions[i-1].replace("\n", " ").replace("\r", "")
        realRegions[i]=region

    if "Argentina" in countries:
        set_south_america_region = True
    if set_south_america_region:
        region = "South/Central America & Carribean"

    if region or countries or cities[i] or dataset:
        data.append([region.replace("\n", " ").replace("\r", ""),
         countries.replace("\n", " ").replace("\r", ""),
          cities[i].replace("\n", " ").replace("\r", ""),
           dataset])

    i += 1

makeFolder()

# write data in csv
filename = "data/data.csv"

with open(filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["Regions", "Countries", "Cities", "Dataset"])
    writer.writerows(data)

print("successfully saved in:", filename)
