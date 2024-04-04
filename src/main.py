from data_collection.main import download_base
from data_preprocessing.main import consolidated_base
from data_preprocessing.main import execute_sql
from data_preprocessing.main import insert_data


def main():
    #download_base()
    #consolidated_base()
    execute_sql("CREATE TABLE IF NOT EXISTS matches (id TEXT PRIMARY KEY, atp TEXT, tournament TEXT, date TEXT, season TEXT, series TEXT, court TEXT, surface TEXT, round TEXT, best_of INTEGER, winner TEXT, loser TEXT, wrank INTEGER, lrank INTEGER, wpts INTEGER, lpts INTEGER, w1 INTEGER, l1 INTEGER, w2 INTEGER, l2 INTEGER, w3 INTEGER, l3 INTEGER, w4 INTEGER, l4 INTEGER, w5 INTEGER, l5 INTEGER, wsets INTEGER, lsets INTEGER, comment TEXT, b365w REAL, b365l REAL);")
    insert_data()

if __name__ == "__main__":
    main()