# IMPORTS LIBRARIES
import pandas as pd

# IMPORT FUNCTIONS FROM MODULES
from modules import argparser as arg
from modules import reporting as rep
from modules import update_data as upd

# DEFINITION
bicimad_path = "data/raw/bicimad_stations.csv"
bicipark_path = "data/raw/bicipark_stations.csv"
public_school_url = 'https://datos.madrid.es/egob/catalogo/202311-0-colegios-publicos.json'


# MAIN FUNCTION
def main():
    """Summary: Function to execute the pipeline to extract, process and return the results of this application. 

    Returns:
        dataframe: dataframe with the result. This can be the complete dataframe or only the one corresponding to one of the schools, which 
        has been previously selected by the user.
    """
    # Read the output of argparser related to how the user wants to obtain the processed data 
    out_argp = arg.argument_parser()
    print()

    # The user has the opcion to choose if he want see update data or data from repository. The first option spend more time, more than 6 min.
    # If the user select calculate from row data. This process update the data.
    if out_argp.processed == 'calculate':
        merge_df = upd.obtain_data(bicimad_path, bicipark_path, public_school_url)
    # If the user select import processed data from the repository. This process use the data previous calculated
    elif out_argp.processed == 'import':
        # Import the data to work with the repostory.
        merge_df = pd.read_csv("data/processed/distance_calculated.csv", sep=',')

    # After that, the user must be choose if want to see all the data related to public schools and nearest stations for each one.
    # If the user select show and export all of the data
    if out_argp.result == 'all':
        # Short and store all data in a '.csv'. In the imput of the function include as parameter the station type (FLAG: "-s")
        return rep.store_all_data(merge_df, out_argp.station)
    # If the user select show only the data related to one of the schools
    else:
        # Show the data related to the school entered by the user.
        return rep.show_one_school(merge_df, out_argp.station, out_argp.result)


# MAIN EXECUTION
if __name__ == '__main__':
    result = main()
    print(result)
