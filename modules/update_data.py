# IMPORT FUNCTIONS FROM MODULES
from modules import wrangling as wra
from modules import analysis as ana


# PIPELINE FUNCTIONS
def obtain_data(bicimad_path, bicipark_path, public_school_url):
    """Summary: with this function it's possible to obtain the data importing them from csv and json. After that, clean and prepare
    the data to use in reporting step. Store the data in a processed '.csv'

    Returns:
        merge_df (dataframe): dataframe with data prepared to use in the reporting step
    """
    # ACQUISITION AND WRANGLING DATA
    # Obtain bicimad dataframe from csv
    bicimad_stations_df = wra.csv2df(bicimad_path, '\t', ' - ', 'BiciMAD')
    # Obtain bicipark dataframe from csv
    bicipark_stations_df = wra.csv2df(bicipark_path, ';', 'Bicipark ', 'BiciPARK')
    # Obtain public schools dataframe from json
    public_schools_df = wra.json2df(public_school_url)
    # Obtain merged dataframe with bicimad, bicipark and public schools with the necessary columns
    merge_df = wra.fix_df(bicimad_stations_df, bicipark_stations_df, public_schools_df)
    
    # ANALYSIS
    merge_df = ana.calculate_distance(merge_df)

    # REPORTING
    # Store the procesed data in csv and this way I won't have to wait for the distance calculations to be performed if I want 
    # to work with the dataframe
    merge_df.to_csv("data/processed/distance_calculated.csv", index=False)

    return merge_df