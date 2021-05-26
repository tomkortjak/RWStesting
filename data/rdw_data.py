from sqlalchemy import create_engine
import requests
import pandas as pd
import dask.dataframe as dd
import data.voertuigenBrandstofCleaning as rdw

# Get the amount of rows from rdw database for the fuel data
url_count_fuel = "https://opendata.rdw.nl/resource/8ys7-d773.json?$select=count(kenteken)"
r = requests.get(url_count_fuel)
data_count = pd.DataFrame(r.json())
count_fuel = data_count["count_kenteken"][0]

# Get the amount of rows from rdw database for the vehicle data
url_count_vehicle = "https://opendata.rdw.nl/resource/m9d7-ebf2.json?$select=count(kenteken)"
r = requests.get(url_count_vehicle)
data_count = pd.DataFrame(r.json())
count_vehicle = data_count["count_kenteken"][0]

chunk_size = 100000
df_fuel = pd.DataFrame()
df_vehicle = pd.DataFrame()

# These 2 for loops both request there respective data(data = 1.fuel  2.vehicle) from the rdw database using json links.
# They then transform there data into dask dataframes and after that clean the data in parallel. This is done in chunks
# which are appended in the end of the loop.
for x in range(0, int(count_fuel) + chunk_size, chunk_size):

    url = "https://opendata.rdw.nl/resource/8ys7-d773.json?$select=kenteken,brandstof_omschrijving," \
          "klasse_hybride_elektrisch_voertuig&$limit={limit}&$offset={offset}&$order=:id"
    url = url.format(offset=x, limit=chunk_size)
    print(url)
    r = requests.get(url)

    data = pd.DataFrame(r.json())
    data = dd.from_pandas(data, npartitions=8)

    if len(data) != 0:
        brandstof = rdw.clean_brandstof(data)
        brandstof = brandstof.compute()
        df_fuel = df_fuel.append(brandstof)

for x in range(0, int(count_vehicle) + chunk_size, chunk_size):

    url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json?$select=kenteken,voertuigsoort,merk,handelsbenaming," \
          "vervaldatum_apk,datum_eerste_afgifte_nederland,catalogusprijs,export_indicator," \
          "taxi_indicator&$limit={limit}&$offset={offset}&$order=:id "
    url = url.format(offset=x, limit=chunk_size)
    print(url)
    r = requests.get(url)

    data = pd.DataFrame(r.json())
    data = dd.from_pandas(data, npartitions=8)

    if len(data) != 0:
        voertuigen = rdw.clean_voertuig(data)
        voertuigen = voertuigen.compute()
        df_vehicle = df_vehicle.append(voertuigen)

# Merges the brandstof and voertuigen dataframes on kenteken
result = pd.merge(df_fuel, df_vehicle, how='inner', on='kenteken')

# Replaces incorrect handelsbenamingen with the correct version.
result = rdw.names_util(result)

# Sends the result dataframe to the databse zmoolens to the table named clean_rdw
engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
result.to_sql(name="clean_rdw", con=engine, if_exists="replace", index=False)
