#v1_0: add sqlite functions and class
#handle missing columns
#return rows added with every run. 
#return errors
# error check for bad connection, couldnt connect tod b, write failure, in general mysql data handling


import json
import sqlite3
import itertools
import pandas as pd
import numpy as np
import math

from pprint import pprint
from pandas import DataFrame
import urllib2
from itertools import islice, chain

import sys
import utilities

global loglevel
global ulog

if len(sys.argv) == 2:
  logLevel = int(sys.argv[1])
else:
  logLevel = 3

ulog = utilities.utilities(logLevel)


def sortdata(pdata, sort_colnames):
    ulog.logit(2, "sortdata... ")
    # TODO Function to write the data_ratings in the JSON format {user_id1: {movie_id,movie_rating}}
    data_habitat_sorted = pdata.sort_values(sort_colnames, ascending = True)   # sorting the data_ratings using the user_id
    return data_habitat_sorted

    
def get_distinct_values(pdf, colnames):
    ulog.logit(2, "get_distinct_values... ")
    # get distinct_userId sorted by User_id
    distinct_userId = pd.unique(pdf[colnames].values.ravel())
    return distinct_userId

def get_alliance_by_playerID(pdf, index, colnames):
    ulog.logit(2, "get_alliance_by_playerID... ")
    # get distinct_userId sorted by User_id
    distinct_userId = pdf.groupby(colnames)
    return distinct_userId
    
def readfile(filename, colnames, alliances):
    ulog.logit(2, "readfile... ")
    # read the full file
    pdata_habitat_raw = pd.read_csv(filename, sep =',', header = None, names = colnames, skiprows = 1, quotechar = '"')
    pdata_habitat = pdata_habitat_raw[pdata_habitat_raw['allianceId'].isin(alliances)]
    return pdata_habitat

def writefile(distance_list, filename):
    ulog.logit(2, "writefile... ")
    with open(filename, mode = 'w') as f:
            json.dump(distance_list, f)
    f.close()

def load_jfile(filename):
    ulog.logit(2, "load_jfile... ")
    with open(filename, mode = 'r') as f:
            distance_list = json.load(f)
    f.close()
    return distance_list
    
def create_habitat_pairs(pdata_unsorted, field_radius):

    ulog.logit(2, "Entering function create_habitat_pairs_and_save... ")
    distance_list = []
    rowCounter = 0
    irowcount = 0
    loop_counter = 0
    sort_colname_coordinates = ['mapX', 'mapY']  # sort columns for pandas dataframe
    
    rowcount = len(pdata_unsorted.index)
    pdata = sortdata(pdata_unsorted, sort_colname_coordinates)  # sort input file contents by X and Y coordinates    
    for index_i, row_i in pdata.iterrows():
        loop_counter += 1
        utilities.show_progress(loop_counter, rowcount)  #show progress on screen
        irowcount+=1
        pointer = irowcount
        x1 = int(row_i['mapX'])
        y1 = int(row_i['mapY'])
        rowCounter = 0
        yrange_trigger = False
        
        for index_j, row_j in pdata[pointer: rowcount].iterrows():
            x2 = int(row_j['mapX'])
            y2 = int(row_j['mapY'])
            rowCounter += 1
            if x2 <= x1 + field_radius:
                if y2 < y1 - field_radius:
                    continue
                else:
                    if y2 >= y1 - field_radius and y2 <= y1 + field_radius:
                        yrange_trigger = True
                        distance = compute_distance(x1, y1, x2, y2)
                        if distance <= field_radius and distance != 0:
                            entry = {'playerID':row_i['playerID'], 'id1':int(row_i['id']), 'id2': int(row_j['id']), 'distance': int(distance)} 
                            # int type casted since
                            #sometimes it gives the error JSON not realizable. the row_i['id'] etc turn out to be numpy.int64 data type
                            distance_list.append(entry)
                    else:
                        continue
            else:
                break

    return distance_list
    ulog.logit(1, "Exiting function create_habitat_pairs_and_save... ")        

def compute_distance(x1, y1, x2, y2):
    ulog.logit(2, "Entering function compute_distance... ")
    distance = 0

    #check if x_C1 is odd and add 0.5 if it is. else no change.
    #the below formula works when x_C1 is int since the last bit is always '1' for odd number
    if y1 % 2 == 1:
        _x1 = x1 + 0.5
    else:
        _x1 = x1 + 0.0
    
    if y2 % 2 == 1:
        _x2 = x2 + 0.5
    else:
        _x2 = x2 + 0.0


    xDelta = abs(_x1 - _x2 )
    yDelta = abs(y1 - y2 + 0.0);
    if yDelta * 0.5 > xDelta:
        distance = yDelta
    else:
        distance = yDelta * 0.5 + xDelta
        
    if(x1 == 16285 and x2 == 16277):
        print x1
        print _x1
        print y1
        print x2
        print _x2
        print y2
        print xDelta
        print yDelta
        print yDelta * 0.5
        print distance
    ulog.logit(1, "Exiting function compute_distance... ")        
    return int(distance)

    
#main flow
def main():

    ulog.logit(3, "Starting Enemy Exposure engine: ")
    
    file_input_alliance_data = "data/habitat_details.csv"  # input file
    colnames_habitat = ['lastUpdated', 'id', 'name', 'mapX', 'mapY', 'points', 'playerID', 'publicType', 'allianceId', 'nick', 'playerPoints']  # for pandas dataframe
    alliances_filter = [8869, 26486]
    outputfile_name = "data/habitat_mapping.json"  #
    file_results = 'data/results-100.csv'
    field_radius = 100
    filter_colname_player_details = ['allianceId', 'playerID', 'nick']

    ulog.logit(3, "Reading input file... ")
    pdata_player = readfile(file_input_alliance_data, colnames_habitat, alliances_filter)  # filter and read input file into pandas dataframe
    ulog.logit(3, "Creating habitat pairs for field radius : " + str(field_radius))
    #distance_list = create_habitat_pairs(pdata_player, field_radius)  # create habitat pairs based on the radius
    #writefile(distance_list, outputfile_name)  # write the habitat pairs into json file for future use
    # get habitat pairs and sort by id1, id2
    pdata_distance_map_raw = pd.read_json(outputfile_name)  # read json file to get habitat pairs
    pdata_distance_map = sortdata(pdata_distance_map_raw, ['id1', 'id2']) # sort by habitat pairs

    pdata_player_details = pdata_player[filter_colname_player_details].drop_duplicates()  # get unique alliance, player, nick

    player_exposure_index = []    
    #for each player, for each of his habitat, find all entries (loop twice to check id1 and id2) and calculate average distance
    ulog.logit(3, "Analyzing players for exposure..." + str(field_radius))
    total_players = len(pdata_player_details.index)
    loop_counter = 0  # initializing loop counter to 0
    
    for indexp, rowp in pdata_player_details.iterrows():
        loop_counter += 1  #
        utilities.show_progress(loop_counter, total_players)  #show progress on screen
        #resetting all values to zero to avoid any cached values from previous iteration
        player_mean_habitat_support = []
        player_median_habitat_support = []
        player_habitat_supports = []
        player_mean_habitat_distance = 0
        player_median_habitat_distance = 0
        player_mean_support = 0
        player_median_support = 0
        player_castle_count = 0
        unsupported_castles = 0

        #get player details
        player = rowp['playerID']
        allianceID = rowp['allianceId']
        nick = rowp['nick']

        pdata_player_habitat = pdata_player[pdata_player['playerID'] == player]  #get all habitats for the current player
        player_castle_count =  len(pdata_player_habitat.index)
        
        for index, row in pdata_player_habitat.iterrows():  # for each habitat
            habitat_support_list = []
            mean_habitat_distance = 0
            median_habitat_distance = 0
            supports_counter = 0

            set1 = pdata_distance_map[pdata_distance_map['id1'] == row['id']]  # pick all habitats pairs(id1, id2) where id1 matches  the current player habitat
            for set_index, set_row in set1.iterrows():  # loop through the filtered habitat pairs from above
                if not any(pdata_player_habitat.id == set_row['id2']):  # if both habitats in the pair do not belong to same player
                    supports_counter += 1  # increment supports counter 
                    habitat_support_list.append(set_row['distance'])  # pick the distance value
            # repeat the above process (steps for id1) for id2
            set2 = pdata_distance_map[pdata_distance_map['id2'] == row['id']]  # pick all habitats pairs(id1, id2) where id2 matches  the current player habitat
            for set_index, set_row in set2.iterrows():
                if not any(pdata_player_habitat.id == set_row['id1']):
                    supports_counter += 1 # increment supports counter 
                    habitat_support_list.append(set_row['distance'])

            # create lists for median and mean distance for the player
            if len(habitat_support_list):  # if the current player castle showed presence of at least one support
                mean_habitat_distance = sum(habitat_support_list)/len(habitat_support_list) # get mean distance for the habitat
                median_habitat_distance = np.median(habitat_support_list) # get median distance for the habitat
                player_mean_habitat_support.append(mean_habitat_distance)
                player_median_habitat_support.append(median_habitat_distance)
                player_habitat_supports.append(supports_counter)
            else:
                unsupported_castles += 1  # else increment counter for unsupported player castles
        if len(player_mean_habitat_support):  # if any of the current player habitats showed presence of at least one support
            player_mean_habitat_distance = sum(player_mean_habitat_support)/len(player_mean_habitat_support) # get mean support distance for the player
            player_median_habitat_distance = np.median(player_median_habitat_support) # get median support distance for the player
            player_mean_support = sum(player_habitat_supports)/len(player_habitat_supports)  # get mean support size
            player_median_support = np.median(player_habitat_supports)
            
        #save the data for the current player as a key,value set and add to player list
        entry = {'allianceID': allianceID,'playerID':player, 'nick':nick, 'castles count': player_castle_count, 'mean': player_mean_habitat_distance, 'median': player_median_habitat_distance, 'unsupported castles': unsupported_castles, 'mean support': player_mean_support, 'median support': player_median_support}  
        player_exposure_index.append(entry)  #add to player list

    exposure_table_unsorted = pd.DataFrame(player_exposure_index)
    sort_output = ['mean', 'median', 'unsupported castles']  # sort columns for output
    exposure_table = sortdata(exposure_table_unsorted, sort_output)  # sort input data
    exposure_table.to_csv(file_results)  # save results to file
        
    ulog.logit(3, "Enemy exposure analysis completed. Open output file '" + file_results + "' to view results.")  # Finished


if __name__ == "__main__":
  main()