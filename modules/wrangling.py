# IMPORTS LIBRARIES
import pandas as pd

# IMPORT FUNCTIONS FROM MODULES
from modules import acquisition as acq


# AUXILIARY FUNCTIONS
def transform_df(df):
    """Summary: Function to transform and clean the dataframe import from csv.

    Args:
        df (dataframe): this is the dataframe which the user want to clean and transform

    Returns:
        df (dataframe): this is the dataframe clean
    """
    # Extract the column, delete '[' and ']'. Split the string using ',' and convert to float. Store this data in two
    # columns: longitude and latitude. Add this two columns to the original dataframe
    temp_df = df['geometry.coordinates'].str.strip('[]').str.split(',', expand=True).astype('float64')
    temp_df.columns = ['longitude', 'latitude']
    df= pd.concat([df,temp_df],axis=1)
    
    # Delete column 'geometry.coordinates' and 'Unnamed: 0' columns
    df = df.drop(['Unnamed: 0', 'geometry.coordinates'], axis=1)
    
    # Change the name of 'geometry.type' column becase include '.' in the name, and it could be a potential error
    df = df.rename(columns={'geometry.type':'geometry_type'})
    
    # In case the column names were e.g. 'stationId', extract each column name, if includes 'station', repleace that for 
    # ' '. And change the string to lowercase
    columns = df.columns.tolist()
    new_column_names = [column_name.replace('station', '').lower() for column_name in columns]
    df.columns = new_column_names
    
    return df


def extract_dict2df(df):
    """Summary: Function to extract the dictionaries that are included in the cells of some of the columns. Create a new column for each key and 
    store in it the corresponding values. Finally delete the original columns where the dictionaries are located.
    With this function it is possible to extract the dictionaries that are in different columns, regardless of the number of columns that have 
    dictionaries or the number of items in each of them.

    Args:
        df (dataframe): dataframe in which some columns includes a dictionary in the cells

    Returns:
        df (dataframe): dataframe with the dictionary data split in diferent columns
    """
    column_names = df.columns.values   # Store the column names in a list called 'column_names'

    # In this loop, iterate over the columns of the DataFrame
    for col_name in column_names:
        # Check if the first cell type is a dictionary and, in this case, check if it includes more than 1 items
        if isinstance(df.at[0, col_name], dict) and len(df.at[0, col_name])>1:
            # Extract the keys from the first dictionary found. Use '.at' to get a single value from the DataFrame.
            keys = list(df.at[0, col_name].keys())
 
            # Iterate over keys and add new columns to the DataFrame
            for key in keys:
                new_col_name = f"{col_name}_{key}"  # Name of new column
                df[new_col_name] = df[col_name].apply(lambda x: x.get(key))

            # Delete the previous column with the dictionaries inside each cell
            df = df.drop(columns=[col_name])
        
    return df


# PIPELINE FUNCTIONS
def csv2df(path, separator, character, station_type):
    """Summary: read the '.csv' file, transform the dataframe and remove some wrong character in the station name column

    Args:
        path (string): path csv file
        separator (string): separator as input to the read csv function, to obtain the correct dataframe
        character (string): character to delete to the station name column
        station_type (string): type of station (BiciMAD or BiciPARK)

    Returns:
        df (dataframe): dataframe clean
    """
    # Read the csv and store the data in a dataframe
    df = pd.read_csv(path, sep=separator)
    # Clean and transform the dataframe
    df = transform_df(df)
    # In the station name column it includes diferents characters before the station name, it's necessary to split and store only the name.
    df['name'] = df['name'].apply(lambda row: row.split(character)[1])
    # Store the dataframe in processed data folder. It will be necessary to operation y the reporting step.
    df.to_csv(f"data/processed/{station_type}.csv", index=False)
    # Create a dictionary with new column names
    new_column_names = {'name': 'station_name', 'address': 'station_location', 'latitude': 'latitude', 'longitude': 'longitude'}
    # Rename the columns
    df = df[list(new_column_names.keys())].rename(columns=new_column_names)
    # Create new column with the type of station
    df['station_type'] = station_type
    
    return df


def json2df(url):
    """Summary: function to extract data from url and extract data from dictionaries included in the json

    Args:
        url (string): url direction where the json is stored

    Returns:
        df_without_dict (dataframe): dataframe with the dictionaries extracted from json
    """
    # Extract dataframe from url
    df = acq.import_json(url)
    # Use the 'extract_dict2df' function to extract the diccionaries included in some columns and create new columns with them.
    df_without_dict = extract_dict2df(df)
    # Store the dataframe in processed data folder. It will be necessary to operation y the reporting step.
    df.to_csv("data/processed/public_schools.csv", index=False)

    return df_without_dict


def fix_df(bicimad_stations_df, bicipark_stations_df, public_schools_df):
    """Summary: function to obtain dataframe ready to calculate the distance between each two points. For that fix the station 
    dataframes, clean school dataframe and merge. 

    Args:
        bicimad_stations_df (dataframe): dataframe with data of bicimad stations
        bicipark_stations_df (dataframe): dataframe with data of bicipark stations
        public_schools_df (dataframe): dataframe with data of public schools

    Returns:
        merge_df (dataframe): _description_
    """
    # Concat bicimad and bicipark dataframes
    stations_df = pd.concat([bicimad_stations_df, bicipark_stations_df])
    # Extract some columns from public_schools_df in a new dataframe and rename this columns
    new_column_names = {'title': 'school_name', 'address_street-address': 'school_location', 
                        'location_latitude': 'latitude', 'location_longitude': 'longitude'}
    schools_df = public_schools_df[list(new_column_names.keys())].rename(columns=new_column_names)
    schools_df['place_type'] = 'Colegios públicos'
    # Merge the station and schools dataframe
    merge_df = pd.merge(schools_df.assign(key=1), stations_df.assign(key=1), on='key').drop('key', axis=1)

    return merge_df
    