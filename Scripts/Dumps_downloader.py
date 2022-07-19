import requests
import os
import tempfile
import py7zr
from bs4 import BeautifulSoup as bs


class DD():
    def __init__(self, url) -> None:
        self.url = url
        pass
    

    def __html_parser(self, url):
        response = requests.get(url)
        soup = bs(response.content, 'html.parser')
        directory_listing = soup.select('tr td a')
        return directory_listing

    def Topic_mapping_generator(self) -> dict:
        s = self.__html_parser(self.url)
        topics_mapping = dict()
        for item in s:
            href = item['href']
            topic = href.split('.')[0]
            if href[-3:] == '.7z' and 'meta' not in href:
                try:
                    topics_mapping[topic].append(href)
                except:
                    topics_mapping[topic] = [href]
        return topics_mapping

    
    def download_and_unzip(self, url: str, dir= None):
        response = requests.get(url)
        print('File download status code: {}'.format(response.status_code))
        if response.status_code == 200:
            temp =tempfile.TemporaryDirectory()
            full_temp_path = os.path.join(temp.name, 'temp_archive.7z')
            open(full_temp_path, "wb").write(response.content)
            with py7zr.SevenZipFile(full_temp_path, mode='r') as z: 
                    z.extractall(dir)
            temp.cleanup()
            return 
        print('Error downloading file from url: {}'.format(url))
    

