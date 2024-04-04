import os
import requests
import pandas as pd
import sqlite3

CSV_DIRECTORY = "../../atp-tour-tennis/data/raw/" # Directory containing your CSV files

def consolidated_base(csv_directory=CSV_DIRECTORY):
    try:
        dataframes = []
        output_csv = "../../atp-tour-tennis/data/processed/apt-tour-consolidated.csv"

        # List from 2002/2003 to 2024 (Actual)
        past_seasons = ["2005", "2006", "2007", "2008", "2009", "2010", "2011",
                        "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"]
        actual_season = ["2024"]

        for season in actual_season:
            # Loop through each CSV file in the directory
            for filename in os.listdir(csv_directory):
                if f"atp-tour-{season}" in filename:
                    file_path = os.path.join(csv_directory, filename)
                    cols_to_read = ["ATP", "Location", "Tournament", "Date", "Series", "Court", "Surface", "Round", "Best of",
                                    "Winner", "Loser", "WRank", "LRank", "WPts", "LPts", "W1", "L1", "W2", "L2", "W3", "L3", "W4", "L4", "W5", "L5", "Wsets", "Lsets", "Comment", "B365W", "B365L"]
                    df = pd.read_excel(file_path, usecols=cols_to_read)
                    df = df.dropna()
                    df = df.astype({"ATP": "string", "Tournament": "string", "Date": "string", "Series": "string", "Court": "string", "Surface": "string", "Round": "string", "Best of": "int64", "Winner": "string", "Loser": "string", "WRank": "int64",
                                "LRank": "int64", "WPts": "int64", "LPts": "int64", "W1": "int64", "L1": "int64", "W2": "int64", "L2": "int64", "W3": "int64", "L3": "int64", "W4": "int64", "L4": "int64", "W5": "int64", "L5": "int64", "Wsets": "int64", "Lsets": "int64", "Comment": "string", "B365W": "float64", "B365L": "float64"})
                    df = df.rename(columns={
                        "ATP": "atp",
                        "Tournament": "tournament",
                        "Date": "date",
                        "Series": "series",
                        "Court": "court",
                        "Surface": "surface",
                        "Round": "round",
                        "Best of": "best_of",
                        "Winner": "winner",
                        "Loser": "loser",
                        "WRank": "wrank",
                        "LRank": "lrank",
                        "WPts": "wpts",
                        "LPts": "lpts",
                        "W1": "w1",
                        "W2": "w2",
                        "W3": "w3",
                        "W4": "w4",
                        "W5": "w5",
                        "Wsets": "wsets",
                        "Lsets": "lsets",
                        "Comment": "comment",
                        "B365W": "B365W",
                        "B365L": "B365L"
                    })
                    dataframes.append(df)
                    print(f"Added season {season}")

        # Concatenate all DataFrames into a single DataFrame
        consolidated_df = pd.concat(dataframes, ignore_index=True)

        # Write the consolidated DataFrame to a new CSV file
        consolidated_df.to_csv(output_csv, index=False, mode='a', header=True)
        print(f"Consolidated CSV file saved as {output_csv}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to download file from. Error: {e}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")


# Create tables in database.
def execute_sql(command: str):
    # Parameters to database URL
    params_dic = {
        "host": "localhost",
        "database": f"../data/processed/apt-tour.db"
    }
    
    try:
        connection = sqlite3.connect(f"{params_dic['database']}")
        cursor = connection.cursor()
        cursor.execute(command)
        cursor.close()
        connection.close()
        print(f"Command was executed with success.")
    except (RuntimeError, TypeError, NameError, ValueError, KeyboardInterrupt) as err:
        print(err)
        pass

# Insert the dataset in database.  
def insert_data(csv_directory=CSV_DIRECTORY):
    params_dic = {
        "host": "localhost",
        "database": f"../data/processed/apt-tour.db"
    }

    try:
        # List from 2002/2003 to 2024 (Actual)
        past_seasons = ["2005", "2006", "2007", "2008", "2009", "2010", "2011",
                        "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"]
        actual_season = ["2024"]

        for season in actual_season:
            for filename in os.listdir(csv_directory):
                print(filename)
                if f"atp-tour-{season}" in filename:
                    file_path = os.path.join(csv_directory, filename)
                    cols_to_read = ["ATP", "Location", "Tournament", "Date", "Series", "Court", "Surface", "Round", "Best of",
                                    "Winner", "Loser", "WRank", "LRank", "WPts", "LPts", "W1", "L1", "W2", "L2", "W3", "L3", "W4", "L4", "W5", "L5", "Wsets", "Lsets", "Comment", "B365W", "B365L"]
                    df = pd.read_excel(file_path, usecols=cols_to_read)
                    df = df.dropna()
                    df = df.astype({"ATP": "string", "Tournament": "string", "Date": "string", "Series": "string", "Court": "string", "Surface": "string", "Round": "string", "Best of": "int64", "Winner": "string", "Loser": "string", "WRank": "int64",
                                    "LRank": "int64", "WPts": "int64", "LPts": "int64", "W1": "int64", "L1": "int64", "W2": "int64", "L2": "int64", "W3": "int64", "L3": "int64", "W4": "int64", "L4": "int64", "W5": "int64", "L5": "int64", "Wsets": "int64", "Lsets": "int64", "Comment": "string", "B365W": "float64", "B365L": "float64"})
                    df = df.rename(columns={
                        "ATP": "atp",
                        "Tournament": "tournament",
                        "Date": "date",
                        "Series": "series",
                        "Court": "court",
                        "Surface": "surface",
                        "Round": "round",
                        "Best of": "best_of",
                        "Winner": "winner",
                        "Loser": "loser",
                        "WRank": "wrank",
                        "LRank": "lrank",
                        "WPts": "wpts",
                        "LPts": "lpts",
                        "W1": "w1",
                        "W2": "w2",
                        "W3": "w3",
                        "W4": "w4",
                        "W5": "w5",
                        "Wsets": "wsets",
                        "Lsets": "lsets",
                        "Comment": "comment",
                        "B365W": "B365W",
                        "B365L": "B365L"
                    })

                    connection = sqlite3.connect(f"{params_dic['database']}")
                    cursor = connection.cursor()
                    name_table = f"matches"

                    for _, row in df.iterrows():
                        row["date"] = str(row["date"])
                        row["id"] = ''.join([row["atp"], row["date"], row["winner"], row["loser"]]).replace(" ", "").replace("'", "").replace(".", "").replace("-", "")                
                        row["season"] = season
                        values = (row["id"], row["atp"], row["tournament"], row["date"], row["season"], row["series"], row["court"], row["surface"], row["round"], row["best_of"], row["winner"], row["loser"], row["wrank"], row["lrank"], row["wpts"], row["lpts"], row["w1"], row["w2"], row["w3"], row["w4"], row["w5"], row["wsets"], row["lsets"], row["comment"], row["B365W"], row["B365L"])
                        cursor.execute(f"INSERT OR IGNORE INTO {name_table} (id, atp, tournament, date, season, series, court, surface, round, best_of, winner, loser, wrank, lrank, wpts, lpts, w1, w2, w3, w4, w5, wsets, lsets, comment, B365W, B365L) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", values)
                    connection.commit()
                    cursor.close()
                    connection.close()
                    print(f'\nInsert with success. {cursor.rowcount}')

    except (RuntimeError, TypeError, NameError, ValueError, KeyboardInterrupt) as err:
        print(err)
        pass