# IMPORTS LIBRARIES
import argparse


# PIPELINE FUNCTIONS
def argument_parser():
    """Summary: function argparser

    Returns:
        args (argparser object): argument parser
    """
    # Create ArgumentParser with the app description
    parser = argparse.ArgumentParser(description = 'This app find the BiciMAD/BiciPARK station closest to a set of public\
    schools')
    # Create message to help to the users
    help_message = 'You have configurate diferent options with this app:\
    You have configurate different parameters with this app:\
    \nSelect the way to obtain the processed data (flag="-p") - optional:\
    \n(1) str="calculate": import and calculate procesed data from raw data\
    \n(2) str="import": import processed data file\
    \n\
    \nSelect the type of stations (flag="-s") - optional: \
    \n(1) str="bicimad".\
    \n(2) str="bicipark".\
    \n\
    \nSelect the data that you want to show (flag="-r") - mandatory: \
    \n(1) str="all": to get the table for every "Place of interest" included in the dataset (or a set of them).\
    \n(2) str=__school_name__: to get the table for a specific "public school" imputed by the user.'

    # Use '-p' as a flag to selecto the option to obtain the processed data
    parser.add_argument('-p', '--processed', type=str, 
                        choices=['calculate', 'import'], help=help_message, default='import', required=False)
    # Use '-s' as a flag to selecto the option to obtain the processed data
    parser.add_argument('-s', '--station', type=str, 
                        choices=['bicimad', 'bicipark'], help=help_message, default='bicimad', required=False)
    # Use '-r' as a flag to select if the result will be all schools or one only school
    parser.add_argument('-r', '--result', help=help_message, type=str , required=True)
    # Obtain argument
    args = parser.parse_args()
    return args

#out = argument_parser()
#print(out)