import pandas as pd
import numpy as np
from sqlalchemy import create_engine

columns = ["merk", "handelsbenaming", "price($)", "range_epa(km)", "range_wltp(km)",
           "battery_pack_capacity(kWh)", "max_charging_power(AC)(kW)", "max_charging_power(DC)(kW)",
           "avg_charging_speed(DC)(km/h)", "efficiency(kWh/100km)"]

col_avg = ["price($)", "range_epa(km)", "range_wltp(km)", "battery_pack_capacity(kWh)",
           "max_charging_power(AC)(kW)", "max_charging_power(DC)(kW)",
           "avg_charging_speed(DC)(km/h)", "efficiency(kWh/100km)"]


# get the average of every version so you only have the mark + handelsbenaming in the end
def get_avg():
    engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
    df = pd.read_sql_query("call getWestCars()", engine)

    df = df.drop('versie', 1)
    df = df.drop("range_nedc(km)", 1)

    avg = pd.DataFrame(columns=columns)
    groups = df.groupby(['merk', 'handelsbenaming'])

    index = 0
    for name, group in groups:
        temp = [name[0], name[1]]
        for i in range(len(col_avg)):
            temp.append(group[col_avg[i]].agg(np.mean))
        avg.loc[index] = temp
        index += 1

    engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
    avg.to_sql(name="avg_ev", con=engine, if_exists="replace", index=False)


# combining the rdw data with the evcompare data and saving the data into the sql table ev_rdw_combine
def combineData():
    engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
    df_rdw = pd.read_sql_query("call getRdwData()", engine)
    df_ev = pd.read_sql_query("call getAvgEV()", engine)

    df_ev["auto"] = df_ev["merk"] + " " + df_ev["handelsbenaming"]
    df = df_ev.drop_duplicates(subset=["auto"])
    columns = df_rdw.columns
    df_new = pd.DataFrame(columns=columns)
    df_new['auto'] = []

    # combining the rdw en evcompare data if the rdw car names the evcompare car names contains
    for i in range(len(df)):
        df1 = df_rdw[
            df_rdw['merk'].str.contains(df['merk'][i]) & df_rdw['handelsbenaming'].str.contains(
                df['handelsbenaming'][i])]
        df1['auto'] = df['merk'][i] + " " + df['handelsbenaming'][i]
        frames = [df_new, df1]

        df_new = pd.concat(frames)

        df_new.reset_index(drop=True, inplace=True)

    # combining the evcompare data into the rdw data with the 'auto' column
    df_ev = df_ev.drop(["merk", "handelsbenaming"], axis=1)
    df_new = df_new.drop_duplicates(subset=['kenteken'], keep='last')
    result = pd.merge(df_new, df_ev, how='inner', on='auto')

    result.to_sql(name="ev_rdw_combine", con=engine, if_exists="replace", index=False)
