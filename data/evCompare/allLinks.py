import requests
from bs4 import BeautifulSoup
import pandas as pd

all_pagen = 9


# Goes through all 9 pages of evcompare to scrape the links from all cars and puts them into getInfoCars()
def allLinks():
    webSet = []
    for i in range(all_pagen):
        webSet.append("https://evcompare.io/cars/?PAGEN_1=" + str(i + 1))

    citiesUrls = []
    for url in webSet:
        citiesUrls.append(url.replace('\n', ''))

    links = []
    for url in citiesUrls:
        # Check if page gives a response back
        getPage = requests.get(url)
        statusCode = getPage.status_code

        if (statusCode == 200):
            soup = BeautifulSoup(getPage.text, 'html.parser')

            for item in soup.findAll('div', class_='col-12 col-sm-6 col-md-4 car-wrap'):
                title = item.find('div', class_='car-image')
                for a in title.findAll('a', href=True):
                    links.append("{}{}".format("https://evcompare.io", a["href"]))

    getInfoCars(links)


# Takes the links and searches for all the different version of that car
# drops the duplicates and puts all the links into allCars.txt
def getInfoCars(links):
    citiesUrls = []
    for url in links:
        citiesUrls.append(url.replace('\n', ''))

    df = pd.DataFrame(columns=["links"])
    index = 0
    for url in citiesUrls:
        getPage = requests.get(url)
        statusCode = getPage.status_code

        if (statusCode == 200):
            soup = BeautifulSoup(getPage.text, 'html.parser')
            collection_url = soup.findAll('div', class_='col-12 col-sm-6 col-md-4 col-xl-3 mt-4')
            if "/collections/various-models/" in url:
                for item in collection_url:
                    title = item.find('div', class_='car-title')
                    for a in title.findAll('a', href=True):
                        df.loc[index] = "https://evcompare.io" + a["href"]
            else:
                df.loc[index] = url
            index += 1

    df = pd.DataFrame.drop_duplicates(df)
    df.to_csv('../../assets/allCars.txt', header=None, index=None, sep=' ', mode='a')
