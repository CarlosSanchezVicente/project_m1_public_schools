# IMPORTS LIBRARIES
import pandas as pd
from fuzzywuzzy import process


# AUXILIARY FUNCTIONS
def short_data(df, station_type):
    """Summary: sorts the dataframe using the distance values from smallest to largest. It then filters the dataframe to keep 
    only the one with the smallest distance, for each of the schools.

    Args:
        df (dataframe): dataframe that includes all the distances from each of the stations to each of the schools.
        station_type (string): type of station to be filtered (bicimad or bicipark).

    Returns:
        result_df (dataframe): sorted and filtered dataframe.
    """
    # Short the dataframe for each school from minimum to maximum of the distance from each station to the school that 
    # corresponds to it. Reset index and remove the new column index created
    short_df = df.sort_values(by=['school_name', 'distance']).reset_index().drop('index', axis=1)
    # To extract only one type of items, it's neccesary to apply a filter
    if station_type == 'bicimad':
        filter = short_df['station_type'] == 'BiciMAD'
    elif station_type == 'bicipark':
        filter = short_df['station_type'] == 'BiciPARK'
    # Obtain the resulting dataframe with the school and bicimad station with minimum distance. As the dataframe is already 
    # sorted, with the distance values from smallest to largest, only the first value for each school needs to be extracted. 
    # To do this, it's neccesary to apply the filter calculated above.
    minimum_df = short_df[filter].groupby('school_name').head(1)

    # Change column names and select the desired columns to adapt the result to the objective
    # Create a dictionary with the old and new column names
    new_columns_names = {'school_name': 'Place of interest',
                        'place_type': 'Type of place',
                        'school_location': 'Place address',
                        'station_name': 'BiciMAD station',
                        'station_location': 'Station location'}
    # Extract the interested columns and rename them.
    result_df = minimum_df[list(new_columns_names.keys())].rename(columns=new_columns_names).reset_index(drop=True)

    return result_df


# PIPELINE FUNCTIONS
def store_all_data(df, station_type):
    """Summary: function for sort, filter an store all data

    Args:
        df (dataframe): dataframe that includes all the distances from each of the stations to each of the schools.
        station_type (string): type of station to be filtered (bicimad or bicipark).

    Returns:
        result_df (dataframe): sorted and filtered dataframe.
    """
    # Short dataframe and extract the columns interesting to the goal
    df = short_data(df, station_type)
    # Store the results in csv file
    df.to_csv("data/result/result.csv", index=False)
    return df


def show_one_school(df, station_type, school_name):
    """Summary: at first, check if the name entered by the user is correct with the 80% accuracy. After that, Extract the bases available
    in the stations (bicimad or bicipark) and include this data in the previous result dataframe.

    Args:
        df (dataframe): dataframe that includes all the distances from each of the stations to each of the schools.
        station_type (string): type of station to be filtered (bicimad or bicipark).
        school_name (string): name of the school entered by the user and of which he wants to know the nearest station.

    Returns:
        result_df (dataframe): result of the operations.
    """
    # Short dataframe and extract the columns interesting to the goal
    df = short_data(df, station_type)

    # Checks if the name of the school is correct and if it corresponds to one of those included in the dataframe. Si no existe no realiza
    # ninguna operación
    # With this function it is possible to get the best match for 'school_name' in the different school names
    best_match = process.extractOne(school_name, df['Place of interest'])
    if best_match[1] >= 80:
        # Import the dataframe processed related to station type introduced by the user. This will be extract the numbers of bicicles available.
        station_df = pd.read_csv(f"data/processed/{station_type}.csv", sep=',')

        # Create a filter with the rows that includes the specific lab
        filter_df = df['Place of interest'] == best_match[0]

        # Evaluate if at least one element in condition is True. If True, it means that there is at least one row that meets the condition. If not
        # the return is a error message
        if filter_df.any():
            # Apply the filter
            result_df = df[filter_df]

            # If the user want to see the bicimad available in a specific station
            if station_type == 'bicimad':
                # Create new filter to obtain the data related to the specific station
                filter_station = station_df['name'] == result_df['BiciMAD station'][0]
                # Create list with only the interesting columns
                interesting_columns = ['total_bases', 'dock_bikes', 'free_bases']
                # Create list with the new names to rename the columns
                new_columns_names = ['Total bases', 'Dock bikes', 'Free bases']

            # If the user want to see the bicipark available in a specific station
            elif station_type == 'bicipark':
                # Create new filter to obtain the data related to the specific station
                filter_station = station_df['name'] == result_df['BiciPARK station'][0]
                # Create list with only the interesting columns
                interesting_columns = ['total_places', 'free_places', 'reserved_places']
                # Create list with the new names to rename the columns
                new_columns_names = ['Total places', 'Free places', 'Reserved places']

            # Extract the free bases from the dataframe and include that with result_df
            result_station_df =station_df[filter_station]
            # To insert data in a dataframe, it's necessary to extract the values of columns in this dataframe fragment and convert to list
            data_to_insert = list(result_station_df[interesting_columns].values)
            # Store this data in the dataframe with the other information
            result_df[new_columns_names] = data_to_insert

            return result_df 

    else: 
        return 'Error: the name of the station you typed was not found'