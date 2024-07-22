import webscraping
import upload_gcs

def main():
    soup=webscraping.get_soup_from_url()  # Extracting HTML content
    links=webscraping.fetch_link_from_soup(soup, 2019)  # Extracting .parquet links
    orglinks = webscraping.organize_files_by_type(links)
    print(orglinks)
    upload_gcs.download_and_store_in_gcs(orglinks, 'raw-first-try')

if __name__ == '__main__':
    main()