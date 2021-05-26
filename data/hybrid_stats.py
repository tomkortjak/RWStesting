import requests
import pandas as pd
import datetime
import mysql.connector

url = "https://opendata.rdw.nl/resource/8ys7-d773.json?$select=klasse_hybride_elektrisch_voertuig,%20Count(*)&$group=klasse_hybride_elektrisch_voertuig"

r = requests.get(url)
data = pd.DataFrame(r.json())

NOVC_FCHV = int(data.loc[data['klasse_hybride_elektrisch_voertuig'] == 'NOVC-FCHV']['Count'])
NOVC_HEV = int(data.loc[data['klasse_hybride_elektrisch_voertuig'] == 'NOVC-HEV']['Count'])
OVC_FCHV = int(data.loc[data['klasse_hybride_elektrisch_voertuig'] == 'OVC-FCHV']['Count'])
OVC_HEV = int(data.loc[data['klasse_hybride_elektrisch_voertuig'] == 'OVC-HEV']['Count'])
not_hybrid = int(data.loc[data['klasse_hybride_elektrisch_voertuig'].isnull()]['Count'])

datum = datetime.date.today().strftime("%d-%m-%Y")

sql = "INSERT INTO `hybrid_stats` (`datum`, `NOVC-FCHV`, `NOVC-HEV`, `OVC-FCHV`, `OVC-HEV`, `not_hybrid`) values (%s, %s, %s, %s, %s, %s)"

mydb = mysql.connector.connect(
    host = 'oege.ie.hva.nl',
    user = "moolens",
    password = "5YqhXv1AAJDQzXtD",
    database = 'zmoolens'
)
mycursor = mydb.cursor()  

row = (
    datum,
    NOVC_FCHV,
    NOVC_HEV,
    OVC_FCHV,
    OVC_HEV,
    not_hybrid
)

mycursor.execute(sql, row)
mydb.commit()
mydb.close()