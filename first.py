#KSENIA_SHUBERT_305

import os
import requests
from tqdm import tqdm
from bs4 import BeautifulSoup

def download_files(url, extension):
    dataset_dir = 'dataset'

    # create folder dataset if it didnt exists
    if not os.path.exists(dataset_dir):
        os.makedirs(dataset_dir)
    else:
        # clear folder "dataset"
        for file in os.listdir(dataset_dir):
            file_path = os.path.join(dataset_dir, file)
            if os.path.isfile(file_path):
                os.remove(file_path)

    response = requests.get(url)
    content = response.text

    soup = BeautifulSoup(content, 'html.parser')
    txt_links = [link.get('href') for link in soup.find_all('a') if link.get('href').endswith(extension)]

    session = requests.Session()

    for link in tqdm(txt_links, desc='Downloading files', unit='file'):
        file_url = requests.compat.urljoin(url, link)
        file_name = os.path.join(dataset_dir, os.path.basename(link))

        # download files
        response = session.get(file_url, stream=True)
        response.raise_for_status()
        
        with open(file_name, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

url = 'https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm'
extension = '.txt'

download_files(url, extension)
