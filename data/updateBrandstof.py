import datetime
from sqlalchemy import create_engine
import pandas as pd
import data.voertuigenBrandstofCleaning as rdw

# End is end of the week
endTime = datetime.date.today()

# Start is beginning of the week
startTime = endTime - datetime.timedelta(days=7)

# Format to url parameter
endTime = endTime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
startTime = startTime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

# Testing url:
url = "https://opendata.rdw.nl/resource/8ys7-d773.json?$select=:*,%20*&$limit=14600000000"
url2 = "https://opendata.rdw.nl/resource/m9d7-ebf2.json?$select=:*,%20*&$limit=14600000000"

# clean brandstof and voertuig data
brandstof = rdw.clean_brandstof(url)
voertuigen = rdw.clean_voertuig(url2)

# combine brandstof and voertuigen
result = pd.merge(brandstof, voertuigen, how='inner', on='kenteken')

engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
result.to_sql(name="clean_rdw_test", con=engine, if_exists="replace", index=False)