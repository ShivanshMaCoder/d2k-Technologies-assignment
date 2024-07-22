import requests
from bs4 import BeautifulSoup
import time

'''
This module is responsible for extracting links form the given link. 
'''

URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"


def get_soup_from_url():
    attempts=3
    for attempt in range(attempts):
        try:
            response = requests.get(URL)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')   
            return soup
        except requests.exceptions.RequestException as e:
            if attempt < attempts - 1:
                print(f"Error fetching data: {e}. Trying fetching data again 2 second. Attempt {attempt + 1}....")
                time.sleep(2)
            else:
                print(f"Error fetching page: {e}. ")
                
    return None 

def fetch_link_from_soup(soup, year):
    parquet_links={}
    element_tag = soup.find('div', id=f'faq{year}')
    if not element_tag:
        print(f"No data found for the year {year}")
        return parquet_links
    else:
        months = element_tag.find_all('p')
        for month in months:
            month_name = month.get_text(strip=True)
            month_links = month.find_next('ul').find_all('a')
            links_list = [link['href'] for link in month_links]
            parquet_links[month_name] = links_list
    return parquet_links

def organize_files_by_type(parquet_links):
    organized_files = {
        'yellow_tripdata': [],
        'green_tripdata': [],
        'fhv_tripdata': [],
        'fhvhv_tripdata': []
    }

    for month, urls in parquet_links.items():
        for url in urls:
            if 'yellow_tripdata' in url:
                organized_files['yellow_tripdata'].append(url)
            elif 'green_tripdata' in url:
                organized_files['green_tripdata'].append(url)
            elif 'fhv_tripdata' in url:
                organized_files['fhv_tripdata'].append(url)
            elif 'fhvhv_tripdata' in url:
                organized_files['fhvhv_tripdata'].append(url)
    
    return organized_files



