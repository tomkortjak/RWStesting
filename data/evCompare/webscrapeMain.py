from allLinks import allLinks
from webscrapeLinksClean import webscrape
from combining import get_avg, combineData

if __name__ == "__main__":
    # getting all the links from the all EV page
    allLinks()
    # scrape the useful info from the links and clean the data
    webscrape()
    # put the avg from every merk + handelsbenaming
    get_avg()
    # combine RDW and evCompare
    combineData()
