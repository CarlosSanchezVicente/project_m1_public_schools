# IMPORTS LIBRARIES
import pandas as pd
import requests
import os
from dotenv import dotenv_values


# AUXILIARY FUNCTIONS
def login_emt(BASE_URL):
    """Summary: function to do login in emt mobility web

    Args:
        BASE_URL (string): string with the base url of the emt web

    Returns:
        accessToken (string): key necessary to extract the updated bicimad data
    """
    # Extract the user data necessary to login
    config = dotenv_values('.env')
    email_user = config.get('CLIENT_ID')   # Extract CLIENT ID from .env file.
    password = config['CLIENT_SECRET']   # Extract CLIENT SECRET from .env file.

    # Built the endpoint and header, make the get operation and extract the token.
    ENDPOINT_LOGIN = "v1/mobilitylabs/user/login/"   # Part of the web adress to login.
    url_login = BASE_URL + ENDPOINT_LOGIN   # Build the endpoint to login.
    headers_longin = {"email": email_user, "password": password}   # Create the headerns needed to include in the get operation.
    kwargs = {"url": url_login, "headers": headers_longin, "timeout": 10}   # Create the arguments to do the get.
    response_emt_login = requests.get(**kwargs)   # Operation get.
    response_emt_login = response_emt_login.json()   # Transform the data to json.
    
    # If the response code is '00' means that the login operation is correct.
    if response_emt_login['code'] == '00':
        accessToken = response_emt_login['data'][0]['accessToken']   # Extract the accessToken from the json.
        os.putenv("ACCESS_TOKEN", accessToken)   # Store the token in '.env' file
        return accessToken
    else:
        # If the communication isn't possible, print this error.
        print('Error in the comunication with the emt web') 


def extract_bicimad_data_emt(BASE_URL, accessToken):
    """Summary: function to extract updated bicimad data.

    Args:
        BASE_URL (string): url base of the emt web.
        accessToken (string): key necessary to extract the updated bicimad data.

    Returns:
        bicimad_data (dictionary): bicimad data.
    """
    # Extract the token necessary to extract data.
    config = dotenv_values('.env')
    accessToken = config.get('ACCESS_TOKEN')   # Extract TOKEN from .env file.

    # Built the endpoint and header, make the get operation and extract the information related to bicimad station.
    ENDPOINT_STATIONS = "v1/transport/bicimad/stations/"   # Part of the web adress to extract bicimad data.
    url_stations = BASE_URL + ENDPOINT_STATIONS   # Build the endpoint to login.
    headers = {"accessToken": accessToken}   # Create the headerns needed to include in the get operation.
    kwargs = {"url": url_stations, "headers": headers, "timeout": 10}   # Create the arguments to do the get.
    response_emt_station = requests.get(**kwargs)   # Operation get.
    response_emt_station = response_emt_station.json()   # Transform the data to json.

    return response_emt_station


def process_json(json_data):
    # The dictionary has two keys: '@context' and '@graph'. And the interesting data are in the value of the second key where
    # other dictionaries are included. Extract both keys in a list called 'keys' -> json_data["@graph"] = json_data[keys[1]].
    keys=list(json_data.keys())
    # Create the dataframe with the data stored in '@graph'. This way, if the name of the dictionary change, it will still work.
    df = pd.DataFrame(json_data[keys[1]])
    return df


# PIPELINE FUNCTIONS
def import_json(url):
    """Summary: extract data from url in a dataframe.

    Args:
        url (string): url direction where the json is stored.

    Returns:
        json_data (dictionary): json data.
    """
    response = requests.get(url)
    # Obtain json data.
    json_data = response.json()
    # Extract the data from the json.
    df = process_json(json_data)
    return df


def import_update_json():
    """Summary: function to extract the data from web. This funcion checks if the token is valid yet, and extract the information. If the token 
       doesn't exist or is expired, make login again to get the token before extract the data.

    Returns:
        df (datatrame): dataframe extract of the web, but it's necessary to clean yet.
    """
    # Extract the token necessary to extract data.
    config = dotenv_values('.env')
    accessToken = config.get('ACCESS_TOKEN')   # Extract TOKEN from .env file.

    BASE_URL = "https://openapi.emtmadrid.es/"   # Base url of the web.
    json__response = extract_bicimad_data_emt(BASE_URL, accessToken)
    
    # Check if the access token is stil valid. If the token is expired, excute the login function again and create new acess token. With that, 
    # the login operation only is executed when the token is expired. 
    if (json__response['code'] != '00') or  (json__response['description'] == 'Error, token not found in cache'):
        print('The token stored is expired. The program will be login again and create new access token')
        accessToken = login_emt(BASE_URL)
        # Execute the function to extract the updated bicimad data again with the new token.
        json__response_data =  extract_bicimad_data_emt(BASE_URL, accessToken)
    
    json_data = json__response['data'][0]   # Extract the bicimad data from the json.
    df = process_json(json_data)
    return df
