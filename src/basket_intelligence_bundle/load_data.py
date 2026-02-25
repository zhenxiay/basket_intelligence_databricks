import sys
from BasketIntelligence.load_season_data import LoadSeasonData

def load_data(data_source: str, season: str):

    loader = LoadSeasonData(season,"project","BasketIntelligence")

    #define the unity catalog and schema before loading
    spark.sql('USE CATALOG basket_intelligence;')

    spark.sql('CREATE SCHEMA IF NOT EXISTS teams;')

    loader.load_data(
        data_source=data_source,
        db_type='fabric_lakehouse',
        name=data_source,
        n_cluster=5,
    )

if __name__ == "__main__":
    # Use task parameters to pass in the data source and season
    # Then load data
    load_data(
        data_source=str(sys.argv[1]), 
        season=str(sys.argv[2])
        )