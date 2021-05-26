# A map partions function to remove the merk from the handelsbenaming
def replace(data):
    for i in range(len(data)):
        data['handelsbenaming'][i] = data['handelsbenaming'][i].replace(data['merk'][i], "")

    return data


# Cleans the data from the fuel data
def clean_brandstof(data):
    # adds empty klasse_hybride_elektrisch_voertuig column if it is not in dataframe.
    if 'klasse_hybride_elektrisch_voertuig' not in data:
        data["klasse_hybride_elektrisch_voertuig"] = ''

    # Drop all columns except these 3 columns
    data = data[['kenteken', 'brandstof_omschrijving', 'klasse_hybride_elektrisch_voertuig']]

    # Remove any cars that don't have the fuel label elektricity
    data = data[(data['brandstof_omschrijving'] == 'Elektriciteit')]

    # Keep only hybrid cars that are able to charge from external source
    data = data[(data['klasse_hybride_elektrisch_voertuig'] != 'NOVC-HEV')]

    return data


def clean_voertuig(data):
    # drop all columns except these 9 columns
    data = data[
        ['kenteken', 'voertuigsoort', 'merk', 'handelsbenaming', 'vervaldatum_apk', 'datum_eerste_afgifte_nederland',
         'catalogusprijs', 'export_indicator', 'taxi_indicator']]

    # Cast handelsbenaming and merk to string
    data['handelsbenaming'] = data['handelsbenaming'].astype(str)
    data['merk'] = data['merk'].astype(str)

    # De-capitalize the Handelsbenaming and Merk collums
    data['handelsbenaming'] = data['handelsbenaming'].str.lower()
    data['merk'] = data['merk'].str.lower()

    # Drop rows which have data missing from handelsbenaming and/or merk
    data['handelsbenaming'] = data['handelsbenaming'].dropna()
    data['merk'] = data['merk'].dropna()

    data = data.reset_index(drop=True)

    # The function 'replace' utilizes the pandas .replace function which is not accessible in dask. Because of this
    # a map partition function has been called to load each partition (pandas dataframe) in and do them separately
    # but in parallel
    data = data.map_partitions(replace, meta=[('kenteken', 'string'), ('voertuigsoort', 'string'), ('merk', 'string'),
                                              ('handelsbenaming', 'string'), ('vervaldatum_apk', 'string'),
                                              ('datum_eerste_afgifte_nederland', 'string'),
                                              ('catalogusprijs', 'string'),
                                              ('export_indicator', 'string'), ('taxi_indicator', 'string')])

    # Keep only Personenauto's and Bedrijfsauto's and fill empty fields
    data = data[data['voertuigsoort'].isin(['Personenauto', 'Bedrijfsauto'])]
    data = data.fillna('0')

    # Change the date string to a more readable format
    string = data['datum_eerste_afgifte_nederland']
    data['datum_eerste_afgifte_nederland'] = string.str[6:8] + '/' + string.str[4:6] + '/' + string.str[0:4]

    string = data['vervaldatum_apk']
    data['vervaldatum_apk'] = string.str[6:8] + '/' + string.str[4:6] + '/' + string.str[0:4]

    return data


# Reads a text file and per line gives the incorrect and correct form of a handelsbenaming. Then it replaces all uses
# of the incorrect form with the correct form
def names_util(data):
    with open('vehicle_names.txt', 'r') as file:
        lines = file.readlines()

        for line in lines:
            names = line.split('=')
            data['merk'] = data['merk'].replace(names[0], names[1].rstrip('\n'))

    return data
