import pandas as pd
from sqlalchemy import create_engine

print("Compare evCompare with RDW\n", file=open("../../assets/ev_vs_rdw.txt", "w"))
print("Cars in the RDW dataset, but not in EVCompare\n", file=open("../../assets/car_not_available.txt", "w"))

engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
df = pd.read_sql_query("call getAllCars()", engine)  # combine
dfRDW = pd.read_sql_query("call getAutoSoortRDW()", engine)  # rdw

# look at how the ev car names are compared to de rdw car names
df['rdw_auto'] = df['merk'] + " " + df['handelsbenaming']
dfRDW['auto'] = dfRDW['merk'] + " " + dfRDW['handelsbenaming']

grouped = df.groupby('auto')

for name, group in grouped:
    print("EV: {} \nRDW: {}\n".format(name, group["rdw_auto"].unique()), file=open("../../assets/ev_vs_rdw.txt", "a"))

print("Total amount of car versions: {}".format(df["rdw_auto"].unique().shape),
      file=open("../../assets/ev_vs_rdw.txt", "a"))

# which cars did we not get from rdw (mostly hybrid cars)
notAV_car = dfRDW[~dfRDW["auto"].isin(df["rdw_auto"])][["merk", "handelsbenaming", "auto"]]

grouped = notAV_car.groupby('merk')
for name, group in grouped:
    print("{}: \n{} \n".format(name, group["auto"].unique()), file=open("../../assets/car_not_available.txt", "a"))

print("Total amount of ev and hybrid car versions missed: {}".format(notAV_car["auto"].unique().shape),
      file=open("../../assets/car_not_available.txt", "a"))
