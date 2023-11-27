# IMPORTS LIBRARIES
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


# AUXILIARY FUNCTIONS
def to_mercator(lat, long):
    """Summary: transform latitude/longitude data in degrees to pseudo-mercator coordinates in metres

    Args:
        lat (float): latitude
        long (float): longitude 

    Returns:
        c (panda serie): pseudo-mercator coordinates in metres
    """
    c = gpd.GeoSeries([Point(lat, long)], crs=4326)
    c = c.to_crs(3857)
    return c

out = to_mercator(40.488, -3.65)
print('----------------> Esta es la salida', out)
print(type(out))

def distance_meters(lat_start, long_start, lat_finish, long_finish):
    """summary: return the distance in metres between to latitude/longitude pair points in degrees 
                (e.g.: Start Point -> 40.4400607 / -3.6425358 End Point -> 40.4234825 / -3.6292625)

    Args:
        lat_start (integer): latitude of starting point
        long_start (integer): longitude of starting point
        lat_finish (integer): latitude of the end point
        long_finish (integer): longitude of the end point

    Returns:
        distance (integer): distance beetween two points
    """
    # 
    start = to_mercator(lat_start, long_start)
    finish = to_mercator(lat_finish, long_finish)
    return start.distance(finish)


    # PIPELINE FUNCTIONS
def calculate_distance(df):
    """Summary: calculate the distance between each school with respect to each bicimad/bicipark 
    station

    Args:
        df (dataframe): dataframe with schools and stations information

    Returns:
        df (dataframe): dataframe with distance columns
    """
    df['distance'] = df.apply(lambda row: distance_meters(row['latitude_x'], row['longitude_x'],
                                                             row['latitude_y'], row['longitude_y']), axis=1)
    return df