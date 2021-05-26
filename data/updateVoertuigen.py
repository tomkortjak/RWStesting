import pandas as pd
import requests
import datetime
import mysql.connector

# End is end of the week
endTime = datetime.date.today()

# Start is beginning of the week
startTime = endTime - datetime.timedelta(days = 7)

# Format to url parameter
endTime = endTime.strftime("%Y-%m-%dT%H:%M:%S.000Z")
startTime = startTime.strftime("%Y-%m-%dT%H:%M:%S.000Z")

# Format url 
# Testing url:
url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json?$$exclude_system_fields=false&$select=:*,%20*&$where=:updated_at%20between%20%27{}%27%20and%20%27{}%27&$limit=1000".format(startTime, endTime)
# Production:  
# url = "https://opendata.rdw.nl/resource/m9d7-ebf2.json?$$exclude_system_fields=false&$select=:*,%20*&$where=:updated_at%20between%20%27{}%27%20and%20%27{}%27&$limit=1000000".format(startTime, endTime)

# Request data and parse to dataframe
r = requests.get(url)
data = pd.DataFrame(r.json())

# TO KEEP: Kenteken, voertuigsoort, merk, handelsbenaming, vervaldatum_apk, datum_eerste_toelating, catalogusprijs, export_indicator
deleteColumns = [
    ':created_at', ':id', ':updated_at', ':version',
    'datum_tenaamstelling', 'bruto_bpm', 'inrichting', 'aantal_zitplaatsen',
    'eerste_kleur', 'tweede_kleur', 'aantal_cilinders', 'cilinderinhoud',
    'massa_ledig_voertuig', 'toegestane_maximum_massa_voertuig',
    'massa_rijklaar', 'maximum_massa_trekken_ongeremd',
    'maximum_trekken_massa_geremd', 'zuinigheidslabel',
    'datum_eerste_afgifte_nederland',
    'wacht_op_keuren', 'wam_verzekerd', 'aantal_deuren',
    'aantal_wielen', 'afstand_hart_koppeling_tot_achterzijde_voertuig',
    'afstand_voorzijde_voertuig_tot_hart_koppeling', 'lengte', 'breedte',
    'europese_voertuigcategorie', 'technische_max_massa_voertuig', 'type',
    'typegoedkeuringsnummer', 'variant', 'uitvoering',
    'volgnummer_wijziging_eu_typegoedkeuring', 'vermogen_massarijklaar',
    'wielbasis', 'openstaande_terugroepactie_indicator',
    'taxi_indicator', 'maximum_massa_samenstelling',
    'aantal_rolstoelplaatsen', 'maximum_ondersteunende_snelheid',
    'api_gekentekende_voertuigen_assen',
    'api_gekentekende_voertuigen_brandstof',
    'api_gekentekende_voertuigen_carrosserie',
    'api_gekentekende_voertuigen_carrosserie_specifiek',
    'api_gekentekende_voertuigen_voertuigklasse', 'plaats_chassisnummer',
    'europese_voertuigcategorie_toevoeging',
    'europese_uitvoeringcategorie_toevoeging', 'laadvermogen',
    'aanhangwagen_autonoom_geremd', 'aanhangwagen_middenas_geremd',
    'oplegger_geremd', 'vervaldatum_tachograaf',
    'maximale_constructiesnelheid_brom_snorfiets',
    'vermogen_brom_snorfiets', 'type_gasinstallatie'
]

for colName in deleteColumns:
    del data[colName]

# Keep only Personenauto's and Bedrijfsauto's and fill empty fields
data = data[data['voertuigsoort'].isin(['Personenauto', 'Bedrijfsauto'])]
data = data.fillna(0)

# Funtion: remove merk from handelsbenaming
def rewriteHandelsbenaming(merk, handelsbenaming):
    if merk in handelsbenaming:
        return handelsbenaming[len(merk)+1:]
    else:
        return handelsbenaming

# Clean data: remove merk from handelsbenaming, lowercase and capitalize strings
data['handelsbenaming'] = data.apply(lambda x: rewriteHandelsbenaming(x['merk'], x['handelsbenaming']), axis = 1)
data['merk'] = data['merk'].str.lower().str.capitalize()
data['handelsbenaming'] = data['handelsbenaming'].str.lower().str.capitalize()

# Creating database connection
mydb = mysql.connector.connect(
    host = 'oege.ie.hva.nl',
    user = "moolens",
    password = "5YqhXv1AAJDQzXtD",
    database = 'zmoolens'
)
mycursor = mydb.cursor()

# Inserting values to database
sql = "INSERT INTO `Sjors_Test_Voertuigen` (`kenteken`, `voertuigsoort`, `merk`, `handelsbenaming`,`datum_eerste_toelating`, `vervaldatum_apk`) values (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `kenteken` = %s"
for voertuig in data.itertuples():
    row = (
        voertuig.kenteken,
        voertuig.voertuigsoort,
        voertuig.merk,
        voertuig.handelsbenaming,
        voertuig.datum_eerste_toelating,
        voertuig.vervaldatum_apk,
        voertuig.kenteken
    )

    mycursor.execute(sql, row)
        
    mydb.commit()
mydb.close()