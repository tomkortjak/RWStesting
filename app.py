from common.dashboard import GraphPlot
from data.data_loader import DataHandler
import pandas as pd

if __name__ == '__main__':
    dh = DataHandler()

    # Get RDW data from MySQL database
    df = dh.get_rdw_ev_data()
    stats = dh.get_stats()

    # Format stats date column to datetime
    stats['datum'] = pd.to_datetime(
        stats['datum'],
        format='%d-%m-%y',
        exact=False)
    stats = stats.sort_values(by=['datum'])

    # Shuffle and lower cars amount for testing
    df = df.sample(frac=1).reset_index(drop=True)

    # Initiate dashboard with loaded dataframe
    plt = GraphPlot(df, stats)
