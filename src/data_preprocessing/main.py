import os
import requests
import pandas as pd
import sqlite3

CSV_DIRECTORY = "../../../../../data/raw/" # Directory containing your CSV files

def consolidated_base(season:str, csv_directory=CSV_DIRECTORY):
    try:
        dataframes = []
        output_csv = "../../../../../data/processed/england/premier-league/PremierLeague-England-Consolidated.csv"

        # Loop through each CSV file in the directory
        for filename in os.listdir(csv_directory):
            print(filename)
            if f"atp-tour-{season}" in filename:
                file_path = os.path.join(csv_directory, filename)
                cols_to_read = ["ATP", "Location", "Tournament", "Date", "Series", "Court", "Surface", "Round", "Best of",
                                "Winner", "Loser", "WRank", "LRank", "WPts", "LPts", "W1", "L1", "W2", "L2", "W3", "L3", "W4", "L4", "W5", "L5", "Wsets", "Lsets", "Comment", "B365W", "B365L"]
                df = pd.read_csv(file_path, usecols=cols_to_read)
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
                    "AR": "away_red_cards",
                    "B365W": "B365W",
                    "B365L": "B365L"
                })
                dataframes.append(df)
                print(f"Added season {season}")

        # Concatenate all DataFrames into a single DataFrame
        consolidated_df = pd.concat(dataframes, ignore_index=True)

        # Write the consolidated DataFrame to a new CSV file
        consolidated_df.to_csv(output_csv, index=False, mode='a', header=False)
        print(f"Consolidated CSV file saved as {output_csv}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to download file from {url}. Error: {e}")
    except Exception as e:
        print(f"Error occurred: {str(e)}")


# Create tables in database.
def create_table_matches(league:str):
    # Parameters to database URL
    params_dic = {
        "host": "localhost",
        "database": f"{league}.db"
    }
    
    try:
        connection = sqlite3.connect(f"{params_dic['database']}")
        cursor = connection.cursor()
        name_table = f"{league.lower()}_matches"
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {name_table} (id TEXT PRIMARY KEY, division TEXT, season TEXT, date TEXT, home TEXT, away TEXT, home_goals_ft INTEGER, away_goals_ft INTEGER, result_ft TEXT, home_goals_ht INTEGER, away_goals_ht INTEGER, result_ht TEXT, referee TEXT, home_shots INTEGER, away_shots INTEGER, home_shots_on_target INTEGER, away_shots_on_target INTEGER, home_fouls INTEGER, away_fouls INTEGER, home_corners INTEGER, away_corners INTEGER, home_yellow_cards INTEGER, away_yellow_cards INTEGER, home_red_cards INTEGER, away_red_cards INTEGER, B365H FLOAT, B365D FLOAT, B365A FLOAT);")
        cursor.close()
        connection.close()
        print(f'{league.lower()} \nTable: {name_table} was created with success.')
    except (RuntimeError, TypeError, NameError, ValueError, KeyboardInterrupt) as err:
        print(err)
        pass