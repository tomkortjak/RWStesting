import pandas as pd
from sqlalchemy import create_engine


class DataHandler:
    @staticmethod
    def readLocalData(file_name):
        df = pd.read_csv(file_name)
        return df

    @staticmethod
    def get_rdw_ev_data():
        engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
        df = pd.read_sql_query("call getAllCars()", engine)
        return df

    @staticmethod
    def get_stats():
        engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
        df = pd.read_sql_query("call getCarStats()", engine)
        return df
