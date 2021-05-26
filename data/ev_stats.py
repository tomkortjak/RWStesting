import requests
import pandas as pd
import datetime
import mysql.connector

url = "https://opendata.rdw.nl/resource/8ys7-d773.json?$select=*&$where=klasse_hybride_elektrisch_voertuig%20IS%20NOT%20NULL&$LIMIT=10000000"

r = requests.get(url)
data = pd.DataFrame(r.json())

data.groupby(['klasse_hybride_elektrisch_voertuig', 'brandstof_volgnummer', 'brandstof_omschrijving']).count()

columns = ['kenteken', 'brandstof_volgnummer', 'brandstof_omschrijving', 'klasse_hybride_elektrisch_voertuig']
data = data[columns]

def cleandf(df, volgnummer):
    df = df.set_index('kenteken')
    if volgnummer == 1:
        df = df.drop(columns = 'klasse_hybride_elektrisch_voertuig')
        df.col_avg = ['brandstof_volgnummer_1', 'brandstof_omschrijving_1']
    else:
        df.col_avg = ['brandstof_volgnummer_2', 'brandstof_omschrijving_2', 'klasse_hybride_elektrisch_voertuig']
    return df

df1 = cleandf(data[data['brandstof_volgnummer'] == '1'], 1)
df2 = cleandf(data[data['brandstof_volgnummer'] == '2'], 2)

total = pd.merge(df1, df2, on = 'kenteken')
total.reset_index(inplace = True),

stats = total.groupby(['brandstof_omschrijving_1', 'brandstof_omschrijving_2']).size().reset_index(name = 'aantal')

benzine_elektriciteit = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Benzine') & (stats['brandstof_omschrijving_2'] == 'Elektriciteit')])
benzine_lng = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Benzine') & (stats['brandstof_omschrijving_2'] == 'LNG')])
diesel_elektriciteit = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Diesel') & (stats['brandstof_omschrijving_2'] == 'Elektriciteit')])
elektriciteit_benzine = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Elektriciteit') & (stats['brandstof_omschrijving_2'] == 'Benzine')])
elektriciteit_cng = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Elektriciteit') & (stats['brandstof_omschrijving_2'] == 'CNG')])
elektriciteit_diesel = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Elektriciteit') & (stats['brandstof_omschrijving_2'] == 'Diesel')])
elektriciteit_lpg = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Elektriciteit') & (stats['brandstof_omschrijving_2'] == 'LPG')])
elektriciteit_waterstof = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Elektriciteit') & (stats['brandstof_omschrijving_2'] == 'Waterstof')])
lng_diesel = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'LNG') & (stats['brandstof_omschrijving_2'] == 'Diesel')])
lng_elektriciteit = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'LNG') & (stats['brandstof_omschrijving_2'] == 'Elektriciteit')])
lpg_benzine = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'LPG') & (stats['brandstof_omschrijving_2'] == 'Benzine')])
waterstof_elektriciteit = int(stats['aantal'][(stats['brandstof_omschrijving_1'] == 'Waterstof') & (stats['brandstof_omschrijving_2'] == 'Elektriciteit')])

datum = datetime.date.today().strftime("%d-%m-%Y")

sql = "INSERT INTO `ev_stats` (`datum`, `benzine_elektriciteit`, `benzine_lng`, `diesel_elektriciteit`, `elektriciteit_benzine`, `elektriciteit_cng`, `elektriciteit_diesel`, `elektriciteit_lpg`, `elektriciteit_waterstof`, `lng_diesel`, `lng_elektriciteit`, `lpg_benzine`, `waterstof_elektriciteit`) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

mydb = mysql.connector.connect(
    host = 'oege.ie.hva.nl',
    user = "moolens",
    password = "5YqhXv1AAJDQzXtD",
    database = 'zmoolens'
)
mycursor = mydb.cursor()  

row = (
    datum,
    benzine_elektriciteit,
    benzine_lng,
    diesel_elektriciteit,
    elektriciteit_benzine,
    elektriciteit_cng,
    elektriciteit_diesel,
    elektriciteit_lpg,
    elektriciteit_waterstof,
    lng_diesel,
    lng_elektriciteit,
    lpg_benzine,
    waterstof_elektriciteit
)

mycursor.execute(sql, row)
mydb.commit()
mydb.close()