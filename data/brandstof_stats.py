import requests
import pandas as pd
import datetime
import mysql.connector

url = "https://opendata.rdw.nl/resource/8ys7-d773.json?$select=brandstof_omschrijving,count(*)&$group=brandstof_omschrijving"

r = requests.get(url)
data = pd.DataFrame(r.json())

datum = datetime.date.today().strftime("%d-%m-%Y")

sql = "INSERT INTO `brandstof_stats` (`datum`, `alcohol`, `benzine`, `cng`, `diesel`, `elektriciteit`, `lng`, `lpg`, `waterstof`) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

mydb = mysql.connector.connect(
    host = 'oege.ie.hva.nl',
    user = "moolens",
    password = "5YqhXv1AAJDQzXtD",
    database = 'zmoolens'
)
mycursor = mydb.cursor()  

row = (
    datum,
    data.iloc[0]['count'],
    data.iloc[1]['count'],
    data.iloc[2]['count'],
    data.iloc[3]['count'],
    data.iloc[4]['count'],
    data.iloc[5]['count'],
    data.iloc[6]['count'],
    data.iloc[8]['count']
)

mycursor.execute(sql, row)
mydb.commit()
mydb.close()