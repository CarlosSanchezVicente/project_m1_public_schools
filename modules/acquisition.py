# IMPORTS LIBRARIES
import pandas as pd
import requests


# PIPELINE FUNCTIONS
def import_json(url):
    """Summary: extract data from url in a dataframe

    Args:
        url (string): url direction where the json is stored

    Returns:
        df (dataframe): dataframe with json data
    """
    response = requests.get(url)
    # Obtain json data
    json_data = response.json()

    # The dictionary has two keys: '@context' and '@graph'. And the interesting data are in the value of the second key where
    # other dictionaries are included. Extract both keys in a list called 'keys' -> json_data["@graph"] = json_data[keys[1]]
    keys=list(json_data.keys())

    # Create the dataframe with the data stored in '@graph'. This way, if the name of the dictionary change, it will still work.
    df = pd.DataFrame(json_data[keys[1]])

    return df
     