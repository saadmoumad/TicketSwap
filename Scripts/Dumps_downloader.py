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
        items_parser = soup.find_all('tr')
        return items_parser

    def Topic_mapping_generator(self) -> dict:
        sites_items = self.__html_parser(self.url)
        sites_mapping = dict()
        for item in sites_items:
            tds = item.find_all('td')
            href = tds[0].a['href']
            site = href.split('.')[0]
            dump_date = tds[1].text
            dump_size = tds[-1].text
            if href[-3:] == '.7z' and 'meta' not in href:
                try:
                    sites_mapping[site][0].append(href)
                except:
                    sites_mapping[site] = [[href], dump_date, dump_size]
        return sites_mapping

    
    def download_and_unzip(self, url: str, dir= None):
        response = requests.get(url)
        print('File download status code: {}'.format(response.status_code))
        if response.status_code == 200:
            temp =tempfile.TemporaryDirectory()
            full_temp_path = os.path.join(temp.name, 'temp_archive.7z')
            open(full_temp_path, "wb").write(response.content)
            with py7zr.SevenZipFile(full_temp_path, mode='r') as z:
                    print('Unziping Files ...') 
                    z.extractall(dir)
            print('DONE')
            temp.cleanup()
            return 
        print('Error downloading file from url: {}'.format(url))
    

