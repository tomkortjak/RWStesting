import pandas as pd
import requests
from bs4 import BeautifulSoup
import numpy as np
from sqlalchemy import create_engine

# webscraping arrays
all_columns = ["merk", "handelsbenaming", "versie", "price($)", "range_epa(km)", "range_nedc(km)", "range_wltp(km)",
               "battery_pack_capacity(kWh)", "max_charging_power(AC)(kW)", "max_charging_power(DC)(kW)",
               "avg_charging_speed(DC)(km/h)", "acceleration(0-100km/h)", "top_speed", "engine_power",
               "engine_torque", "efficiency(kWh/100km)", "drive_type", "motor_type", "number_of seats",
               "dimensions(LxWxH)", "wheelbase", "curb_weight", "body_style", "cargo_volume", "towing_capacity"]

useful_columns = ["merk", "handelsbenaming", "versie", "price($)", "range_epa(km)", "range_nedc(km)", "range_wltp(km)",
                  "battery_pack_capacity(kWh)", "max_charging_power(AC)(kW)", "max_charging_power(DC)(kW)",
                  "avg_charging_speed(DC)(km/h)", "efficiency(kWh/100km)"]

# Cleaning arrays
cleaning_col = ["range_epa(km)", "range_wltp(km)", "battery_pack_capacity(kWh)",
                "max_charging_power(AC)(kW)", "max_charging_power(DC)(kW)", "avg_charging_speed(DC)(km/h)",
                "efficiency(kWh/100km)"]

col_int = ["price($)", "range_epa(km)", "range_wltp(km)", "max_charging_power(DC)(kW)",
           "avg_charging_speed(DC)(km/h)", "efficiency(kWh/100km)"]

col_float = ["battery_pack_capacity(kWh)", "max_charging_power(AC)(kW)"]


# to lower the amount of dimentions in an array
def smaller(list):
    a = np.array(list)
    b = a.flatten()
    return b


# webscrape all the information from the links in allCars.txt
def webscrape():
    # web scraping all different electrical vehicles from https://evcompare.io/
    citiesUrls = []
    with open('../../assets/allCars.txt') as webSet:
        for url in webSet:
            citiesUrls.append(url.replace('\n', ''))

    df = pd.DataFrame(columns=all_columns)
    index = 0
    for url in citiesUrls:
        # Check if page gives a response back
        getPage = requests.get(url)
        statusCode = getPage.status_code

        if (statusCode == 200):
            soup = BeautifulSoup(getPage.text, 'html.parser')
            temp = []

            carName = soup.find('h1')
            versie = ''
            if carName.find('span'):
                for span in carName.find_all('span', recursive=False):
                    versie = span.text
                    carName = carName.text.replace(versie, '')
            else:
                carName = carName.text

            merk = carName.split()[0]
            carName = carName.replace(merk, '')

            temp.append([merk])
            temp.append([carName])
            temp.append([versie])

            price = soup.find('div', class_='car-props_value')
            temp.append([price.text])

            for item in soup.findAll('td', class_='thisCarPropertyValue'):
                temp.append([item.text])

            if len(temp) is len(all_columns):
                df.loc[index] = smaller(temp)
                index += 1
        else:
            print("Page doesn't respond")

    df = df.replace('\n', ' ', regex=True)
    df = df.filter(useful_columns)

    clean_webscraping(df)


# Clean the data and put then into the sql table electric_cars_west
def clean_webscraping(df):
    df["avg_charging_speed(DC)(km/h)"] = df["avg_charging_speed(DC)(km/h)"].str.replace('~', '')
    df["price($)"] = df["price($)"].str.replace(',', '')

    for col in cleaning_col:
        temp = df[col].str.split(expand=True)
        df[col] = temp[0]

    # All the nan data becomes 0, because np.nan is a float and not an int
    df[cleaning_col] = df[cleaning_col].replace('N/A', 0)
    df["price($)"] = df["price($)"].replace(np.nan, 0)

    df[col_int] = df[col_int].astype(int)
    df[col_float] = df[col_float].astype(float)

    df['handelsbenaming'] = df['handelsbenaming'].str.lower()
    df['merk'] = df['merk'].str.lower()
    df['versie'] = df['versie'].str.lower()
    df['merk'] = df['merk'].str.replace('vw', 'volkswagen')
    df['handelsbenaming'] = df['handelsbenaming'].str.replace('electric', '')
    df['handelsbenaming'] = df['handelsbenaming'].str.replace('e-', '')
    df['handelsbenaming'] = df['handelsbenaming'].str.replace('ev', '')
    df['handelsbenaming'] = df['handelsbenaming'].str.strip()

    engine = create_engine('mysql+pymysql://moolens:5YqhXv1AAJDQzXtD@oege.ie.hva.nl/zmoolens')
    df.to_sql(name="electric_cars_west", con=engine, if_exists="replace", index=False)
