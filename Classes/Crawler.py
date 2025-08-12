import json
import os
import wget
import time
import csv
import requests
import math
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from enum import Enum
import logging

logging.basicConfig(level=logging.INFO)


# Load variables from .env into the environment
load_dotenv("C:\Thesis V4\.env")


class NecessaryInformation(Enum):
    URL = "https://api.github.com/search/repositories?q="  # Base URL for GitHub API
    QUERY = "topic:microservices"  # Query to filter repositories with "microservices" topic
    PARAMETERS = "&per_page=100"  # Default 100 items per page
    DELAY_BETWEEN_QUERIES = 10  # Delay to avoid rate limits
    OUTPUT_FOLDER = "C:/Thesis V4/Output"  # Folder for storing ZIP files
    OUTPUT_CSV_FILE = "C:/Thesis V4/Report/repositories-for-microservices.csv"  # Path for CSV file
    OUTPUT_EXCEL_FILE = "C:/Thesis V4/Report/repositories-summary.xlsx"  # Path for Excel file
    csv_file = open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8')
    start_date = datetime(2020, 1, 1)  # Adjust start date as needed
    end_date = datetime(2020, 6, 30)  # 6-month intervals

    finish_date = datetime(2021, 1, 30)


class DataHandler():
    def __init__(self):
        self.summary_data = []
        self.countOfRepositories = 0
        self.test = NecessaryInformation.URL
        
        self.repositories = csv.writer(NecessaryInformation.csv_file, delimiter=',')
        self.repositories.writerow(['Username', 'Repository Name', 'URL', 'Download Status'])


        
class Crawler():
    def __init__(self, token):
        self.token = token
        self.HEADERS = {"Authorization": f"token {self.token}"}
        self.period_download_count = 0
        self.not_downloaded_repositories = 0

    def getUrl(url):
        """ Given a URL it returns its body """
        response = requests.get(url, headers=HEADERS)
        return response.json()

    def crawl(start_date, end_date, finish_date):
        while start_date < finish_date:
            if end_date > finish_date:
                end_date = finish_date
            logging.info(end_date)
            formatted_start = start_date.strftime("%Y-%m-%d")
            formatted_end = end_date.strftime("%Y-%m-%d")
            # Update the query with the date range
            logging.info(f"Processing repositories created from {formatted_start} to {formatted_end}...")
            url = f"{URL}{QUERY} created:{formatted_start}..{formatted_end}{PARAMETERS}"
            data = getUrl(url)
            # Calculate number of pages needed within this date range
            numberOfPages = int(math.ceil(data.get("total_count", 0) / 100.0))
            logging.info(f"Total pages to process in this date range: {numberOfPages}")
            # Process each page of results for the current date range
            for currentPage in range(1, numberOfPages + 1):
                logging.info(f"Processing page {currentPage} of {numberOfPages}...")
                paged_url = url + "&page=" + str(currentPage)
                data = getUrl(paged_url)
                
                # Loop through each repository on the current page
                for item in data.get('items', []):
                    user = item['owner']['login']
                    repository = item['name']
                    repo_url = item['clone_url']
                    topics = item.get('topics', [])

                    # Check if "microservices" is indeed listed as a topic
                    if "microservices" in topics:
                        logging.info(f"Downloading repository '{repository}' from user '{user}'...")
                        fileToDownload = repo_url[:-4] + "/archive/refs/heads/master.zip"
                        fileName = item['full_name'].replace("/", "#") + ".zip"

                        # Try downloading the ZIP file and logging the result
                        try:
                            wget.download(fileToDownload, out=OUTPUT_FOLDER + "/" + fileName)
                            repositories.writerow([user, repository, repo_url, "downloaded"])
                            period_download_count += 1
                        except Exception as e:
                            logging.info(f"Could not download file {fileToDownload}")
                            logging.info(e)
                            not_downloaded_repositories += 1
                            repositories.writerow([user, repository, repo_url, "error when downloading"])

                        
                        countOfRepositories += 1
                    else:
                        logging.info(f"Skipping '{repository}' as it does not have the 'microservices' topic.")
                
                # Delay between pages to comply with rate limits
                logging.info(f"Sleeping {DELAY_BETWEEN_QUERIES} seconds before the next page...")
                time.sleep(DELAY_BETWEEN_QUERIES)
            
            # Store summary data for this period
            summary_data.append([formatted_start, formatted_end, period_download_count, numberOfPages, not_downloaded_repositories])
            logging.info(f"Total repositories downloaded in this period: {period_download_count}")

            # Move to the next 6-month period
            start_date = end_date + timedelta(days=1)
            end_date = start_date + timedelta(days=182)  # Move 6 months ahead
        
        
    


# Constants 
token = os.getenv("GITHUB_TOKEN")
GITHUB_TOKEN = token  # Replace with your GitHub personal access token
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"}
# Constants 
URL = "https://api.github.com/search/repositories?q="  # Base URL for GitHub API
QUERY = "topic:microservices"  # Query to filter repositories with "microservices" topic
PARAMETERS = "&per_page=100"  # Default 100 items per page
DELAY_BETWEEN_QUERIES = 10  # Delay to avoid rate limits
OUTPUT_FOLDER = "C:\Thesis V3\Output"  # Folder for storing ZIP files
OUTPUT_CSV_FILE = "C:/Thesis V3/repositories-for-microservices.csv"  # Path for CSV file
OUTPUT_EXCEL_FILE = "C:/Thesis V3/repositories-summary.xlsx"  # Path for Excel file
# Functions 
def getUrl(url):
    """ Given a URL it returns its body """
    response = requests.get(url, headers=HEADERS)
    return response.json()

# Counter for processed repositories
countOfRepositories = 0
summary_data = []  # List to store summary for each 6-month period

# CSV file initialization for repository metadata
csv_file = open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8')
repositories = csv.writer(csv_file, delimiter=',')
repositories.writerow(['Username', 'Repository Name', 'URL', 'Download Status'])

# Starting date and ending date for 6-month increments
start_date = datetime(2020, 1, 1)  # Adjust start date as needed
end_date = datetime(2020, 6, 30)  # 6-month intervals

finish_date = datetime(2021, 1, 30)

while start_date < finish_date:

    if end_date > finish_date:
        end_date = finish_date
    logging.info(end_date)
    formatted_start = start_date.strftime("%Y-%m-%d")
    formatted_end = end_date.strftime("%Y-%m-%d")
    
    # Update the query with the date range
    logging.info(f"Processing repositories created from {formatted_start} to {formatted_end}...")
    url = f"{URL}{QUERY} created:{formatted_start}..{formatted_end}{PARAMETERS}"
    data = getUrl(url)
    # Calculate number of pages needed within this date range
    numberOfPages = int(math.ceil(data.get("total_count", 0) / 100.0))
    logging.info(f"Total pages to process in this date range: {numberOfPages}")
    # logging.info(data)
    period_download_count = 0  # Counter for this period
    not_downloaded_repositories = 0 

    # Process each page of results for the current date range
    for currentPage in range(1, numberOfPages + 1):
        logging.info(f"Processing page {currentPage} of {numberOfPages}...")
        paged_url = url + "&page=" + str(currentPage)
        data = getUrl(paged_url)
        
        # Loop through each repository on the current page
        for item in data.get('items', []):
            user = item['owner']['login']
            repository = item['name']
            repo_url = item['clone_url']
            topics = item.get('topics', [])

            # Check if "microservices" is indeed listed as a topic
            if "microservices" in topics:
                logging.info(f"Downloading repository '{repository}' from user '{user}'...")
                fileToDownload = repo_url[:-4] + "/archive/refs/heads/master.zip"
                fileName = item['full_name'].replace("/", "#") + ".zip"

                # Try downloading the ZIP file and logging the result
                try:
                    wget.download(fileToDownload, out=OUTPUT_FOLDER + "/" + fileName)
                    repositories.writerow([user, repository, repo_url, "downloaded"])
                    period_download_count += 1
                except Exception as e:
                    logging.info(f"Could not download file {fileToDownload}")
                    logging.info(e)
                    not_downloaded_repositories += 1
                    repositories.writerow([user, repository, repo_url, "error when downloading"])

                
                countOfRepositories += 1
            else:
                logging.info(f"Skipping '{repository}' as it does not have the 'microservices' topic.")
        
        # Delay between pages to comply with rate limits
        logging.info(f"Sleeping {DELAY_BETWEEN_QUERIES} seconds before the next page...")
        time.sleep(DELAY_BETWEEN_QUERIES)
    
    # Store summary data for this period
    summary_data.append([formatted_start, formatted_end, period_download_count, numberOfPages, not_downloaded_repositories])
    logging.info(f"Total repositories downloaded in this period: {period_download_count}")

    # Move to the next 6-month period
    start_date = end_date + timedelta(days=1)
    end_date = start_date + timedelta(days=182)  # Move 6 months ahead



# Completion summary
logging.info("DONE! Processed repositories:", countOfRepositories)
csv_file.close()

# Save summary data to Excel
summary_df = pd.DataFrame(summary_data, columns=['Start Date', 'End Date', 'Downloaded Repositories', 'Number of Pages', 'Number of Failed Downloads'])
summary_df.to_excel(OUTPUT_EXCEL_FILE, index=False)
logging.info("Summary saved to Excel file.")
