import os
import requests
import pandas as pd
import sqlite3


CSV_DIRECTORY = "../../../data/raw/" # Directory containing your CSV files

def download_base(url, output_file):
    try:
        response = requests.get(url)
        # Raise an HTTPError for bad responses (4xx and 5xx status codes)
        response.raise_for_status()

        with open(output_file, "wb") as file:
            file.write(response.content)

        print(f"File downloaded successfully as {output_file}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to download file from {url}. Error: {e}")


# List from 2002/2003 to 2024 (Actual)
past_seasons = ["2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011",
                "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"]
actual_season = ["2024"]

#create_table_matches(league="premier_league")

for season in actual_season:
    url = f"http://www.tennis-data.co.uk/{season}/{season}.xls"
    output_file = f"/home/felipe/Documents/tier0/atp-tour-tennis/data/raw/atp-tour-{season}.xls"
    print(f"Downloading file from {url} to {output_file}")
    download_base(url, output_file)
    #insert_data(season, league="premier_league") # 8263 rows