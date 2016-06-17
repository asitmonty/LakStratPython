#v1_02: working file for creating player growth tracker
#v1_03: Adding json creation for habitats cluster

import sqlite3
import pandas
import numpy as np
from datetime import datetime
import os
import aws_s3

#import plotly.tools as tls
#import plotly.plotly as py
#from plotly.tools import FigureFactory as FF 
import json
import time
import sys
import numpy


import databasehelper_mysql as dbhelper
import datasourceActivityTracker
import datasourceActivityTrackerChange
import datasourceHabitats
import utilities
import target_finder as clusterizer

global loglevel
global ulog
global tbl
global tbl_habitat
global folderpath
global lastUpdated
global lastUpdateDate

global bucket_name


date_split_character = ' '
folderpath = ''
lastUpdated = ''
lastUpdateDate = ''

COLUMN_LASTUPDATE_LNK = "lastUpdateLnk"
COLUMN_LASTUPDATED = "lastSnapshot"
COLUMN_WORLD = "world"
COLUMN_ID = "id"
COLUMN_NAME = "name"
COLUMN_MAPX = "mapX"
COLUMN_MAPY = "mapY"
COLUMN_POINTS = "points"
COLUMN_PLAYERID = "playerID"
COLUMN_PUBLICTYPE = "publicType"
COLUMN_ALLIANCEID = "allianceId"
COLUMN_ALLIANCENAME = "allianceName"
COLUMN_ALLIANCERANK = "allianceRank"
COLUMN_NICK = "nick"
COLUMN_PLAYERPOINTS = "playerPoints"
COLUMN_PLAYERPOINTS_TOTAL = 'total'


bucket_name='lakstrat'
CASTLE_MAX_POINTS = 290

tbl_habitat = datasourceHabitats.TblHabitat()
tbl_activity_tracker = datasourceActivityTracker.TblActivityTracker()
tbl_activity_tracker_change = datasourceActivityTrackerChange.TblActivityTracker()

if len(sys.argv) == 2:
  logLevel = int(sys.argv[1])
else:
  logLevel = 3

ulog = utilities.utilities(logLevel)


def make_sure_path_exists(path):
    import os
    import errno

    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def add_formatted_date_column(df):
  df["date"] = pandas.to_datetime(df[COLUMN_LASTUPDATED].str.split(date_split_character).str.get(0))  #get the date portion as new column
  df['date'] = df['date'].map(pandas.Timestamp.date)
  return df

  

''' # Function: get_pivoted_dates_table
    # creates a pivoted table from the input dataframe 
    # Inputs : 
                1. 'df' :- dataframe from a sql read query
                                                                                
                player id   player name     alliance id     alliance name       snapshot_date   Player_Points      alliance_rank
                ---------   -----------     ------------    --------------      --------------   -------------     -------     
                3373        Blade           26425           Legends             90346               90693           90246      
                236         Coffeen         26425           Legends             180232              181907          183028     
    # Output:
                1. pivoted dataframe. with dates in columns and player Ids in rows. values equal to playerpoints
                            Date 1      Date 2      Date 3      Date 4
                player id   
                ---------   -------     -------     -------     ------
                3373        90346       90693       90246       100372
                236         180232      181907      183028      189789
'''
def get_pivoted_dates_table(df):

    #pivot dataframe to keep player id as rows and dates as columns
    df_pivot = df.pivot(index=COLUMN_PLAYERID, columns=COLUMN_LASTUPDATED, values=COLUMN_PLAYERPOINTS) # pivot to create table with 
                        #player id as rows index and dates as columns
    #current column dates are in two digit months format. change them to three character month names format for easy reading/display
    snapshot_dates = []
    column_names = list(df_pivot.columns.values)

    #Asit Jun 3 2016 : 
    #commenting conversion of dates to pretty print format. the conversion will now be done in android directly
    #for date in column_names:
    #    date = datetime.strptime(date.astype(datetime), "%Y-%m-%d")  
        #date = date.map(pandas.Timestamp.date)
    #    snapshot_dates.append(datetime.strftime(date, "%Y-%b-%d"))
    #df_pivot.columns = snapshot_dates  #important to rename dates after pivot, since Apr will come before March otherwise
     # date conversion is useful when printing dataframe directly to output.
            # for writing to sql table, you can ignore that and keep it usual yyyy-mm-dd

    #Sort all player Ids by current points
    row_count, column_count = df_pivot.shape
    last_snapshot_column_name = df_pivot.columns.values[column_count - 1]  #pick the last snapshot date value
    df_pivot = df_pivot.sort_values(last_snapshot_column_name, ascending = False)  #sort all values by current points as per the last snapshot date

    #filter list to current players only (those who have presence as of the last snapshot date)
    # else it gives an error for previous players. 
    df_pivot = df_pivot[np.isfinite(df_pivot[last_snapshot_column_name])]
    
    #filter to remove players that are worth <199 points. Since their castles dont hold much value to cap or care about
    df_pivot = df_pivot[df_pivot[last_snapshot_column_name] >199 ]
    return df_pivot
  
  
''' # Function: get_player_growth_table
    # creates the activity/growth table (points change/growth from one date to the next)  for a given sql select output (as dataframe) 
    # Inputs : 
                1. pivoted dataframe. with dates in columns and player Ids in rows. values equal to playerpoints
                            Date 1      Date 2      Date 3      Date 4
                player id   
                ---------   -------     -------     -------     ------
                3373        90346       90693       90246       100372
                236         180232      181907      183028      189789
    # Output:
                1. pivoted dataframe with points growth delta. with dates in columns and player Ids in rows. values equal to playerpoints
                            Date 1      Date 2      Date 3      Date 4
                player id   
                ---------   -------     -------     -------     ------
                3373        -383              0         764         90
                236          982            -83      -20000         0
'''
def get_player_growth_table(df):

  # Create two dataframes from the original ones. first one has the first date column removed, and the second one has last date column removed
  # subtracting second from first will result in a dataframe that represents the points delta from one date to the next in the original frame
  # for e.g. if original dataframe has columns - Feb, Mar, Apr, May, then the resulting dataframe would 
                                #show - (Mar -Feb), (Apr - Mar), (May - Apr)
  
  # get all column names, cut and store the first column name (earliest date) to be used later
  column_names = df.columns.tolist()  #get list of all dates
  initial_date = column_names.pop(0)  #cut and store the first column name (earliest date)
  row_count, column_count = df.shape
  df_2nd_to_last = df.copy()  #create a fresh copy of the original dataframe
  df_2nd_to_last = df_2nd_to_last[column_names]  #filter the new dataframe to keep all columns except the first date (compared to the original dataframe)

  # create another copy with last column removed (and rename columns like the new dataframe created above)
  df_1st_to_secondlast = df.copy()
  column_name_last = df_1st_to_secondlast.columns.values[column_count - 1]
  del df_1st_to_secondlast[column_name_last]
  df_1st_to_secondlast.columns = column_names

  # subtract the second dataframe from the first to get the points delta from one date to the next
  df_diff = df_2nd_to_last - df_1st_to_secondlast
  
  #insert back first column from original in the new to represent initial starting points
  df_diff.insert(0, initial_date, df[initial_date])
  df_diff[COLUMN_PLAYERPOINTS_TOTAL] = df_diff.sum(axis = 1)

  return df_diff

  
''' # add_player_details_to_index
    # reset the index with player details (current index only has player id)
    # Inputs : 
                1. 'df' :- the dataframe that was read from sql query
                                                                                
                player id   player name     alliance id     alliance name       snapshot_date   Player_Points      alliance_rank
                ---------   -----------     ------------    --------------      --------------   -------------     -------     
                3373        Blade           26425           Legends             90346               90693           90246      
                236         Coffeen         26425           Legends             180232              181907          183028     
                
                2. 'df_pivot' :- the dataframe that contains player points growth by period
                            Date 1      Date 2      Date 3      Date 4
                player id   
                ---------   -------     -------     -------     ------
                3373        -383              0         764         90
                236          982            -83      -20000         0
                
    # Output:
                1. activity table as dataframe. rows have player id and related details, columns are the snapshot dates 
                                                                                Date 1      Date 2      Date 3      Date 4
                player id   player name     alliance id     alliance name
                ---------   -----------     ------------    --------------      -------     -------     -------     ------
                3373        Blade           26425           Legends             -383              0         764         90
                236         Coffeen         26425           Legends              982            -83      -20000         0
'''

def add_player_details_to_index(df_player_growth, df_player_data):

    # reset the index for df_pivot to move playerid to columns
    df_growth = df_player_growth.reset_index()
    # merge the df_growth dataframe with the original frame using playerid
    player_colnames = [COLUMN_ALLIANCERANK, COLUMN_ALLIANCENAME, COLUMN_ALLIANCEID, COLUMN_PLAYERID, COLUMN_NICK]  #list all columns to add to index (current index is only player id)
    # select the required columns from the first dataframe and drop duplicates
    df_details = df_player_data[player_colnames].drop_duplicates(COLUMN_PLAYERID, keep='last')
    # merge the details dataframe and the growth dataframe using playerId as common key
    df_merged_indexed = pandas.merge(df_details, df_growth, left_on=[COLUMN_PLAYERID], right_on=[COLUMN_PLAYERID])
    #filter the merged dataframe to exclude NaN values on latest snapshot
    row_count, column_count = df_merged_indexed.shape
    last_column_with_total = df_merged_indexed.columns.values[column_count - 1]  #pick the last collumn that has total playerpoints
    df_merged_indexed = df_merged_indexed.sort_values([COLUMN_ALLIANCEID,last_column_with_total], ascending = [True, False])  #sort all values by current points as per the last snapshot date
    #filter list to current players only (those who have presence as of the last snapshot date)
    # else it gives an error for previous players. 
    df_merged_indexed = df_merged_indexed[np.isfinite(df_merged_indexed[last_column_with_total])]

    player_colnames_new = player_colnames + [COLUMN_PLAYERPOINTS_TOTAL]
    df_merged_indexed = df_merged_indexed.set_index(player_colnames_new)  # set the index by using selected columns
    df_merged_indexed.index.name = player_colnames_new  #set index name
    return df_merged_indexed
  
''' # generatePlayerGrowthTracker
    # generate a dataframe containing player points growth for given players
    # Inputs : 
                1. 'df' :- list of alliances for which player growth needs to be calculated
                
    # Output:
                1. json formatted data in 'index' formatting
'''
def generatePlayerGrowthTracker(allianceIDs):
    # get list of dates first (current date, last week, 5 last months)
    # eventually recreate the tracking table (drop the last reading if greater than the activity period) and add current
    
    # at present activity tracker table is created beforehand directly in SQL
    # TODO function to create/refresh/update the activity tracker table using tbl_player and tbl_alliance
    
    # read data from sql and create an activity table
    df_player_data = tbl_activity_tracker.read_from_sql_to_dataframe(0, allianceIDs.values())  # load selected rows fromt the player activity tracker table to a dataframe
    df_player_points = get_pivoted_dates_table(df_player_data)  #get pivoted dataframe with dates as columns and playerIds as rows
    df_player_growth = get_player_growth_table(df_player_points)  #rework the pivoted dataframe to get points growth instead of raw points
    df_complete_player_growth = add_player_details_to_index(df_player_growth, df_player_data)  # reset indexes to get a spreadsheet style table with full details in rows
    # write the spreadsheet type tables to csv files for easy validation/use where json from next step cannot be used
    ulog.logit(3, "Writing player growth tracker to csv file")
    df_complete_player_growth.to_csv('activity_change_comma.csv', encoding='utf-8')
    
    #TODO for some reason reset_index doesnt default the column names from index names. so getting index_names, column_names and renaming the columns after reset_index. else could have used reset_index directly in pandas.melt
    index_names = df_complete_player_growth.index.name
    column_names = list(df_complete_player_growth.columns.values)
    df_complete_player_growth = df_complete_player_growth.reset_index()
    df_complete_player_growth.columns = index_names + column_names
    df_complete_player_growth = pandas.melt(df_complete_player_growth, id_vars=index_names, value_vars=column_names, var_name=COLUMN_LASTUPDATED, value_name=COLUMN_PLAYERPOINTS)
    column_names = df_complete_player_growth.columns.values
    df_complete_player_growth = df_complete_player_growth.sort_values([COLUMN_ALLIANCEID,COLUMN_PLAYERPOINTS_TOTAL, COLUMN_PLAYERID], ascending = [True, False, True])  #sort all values by current points as per the last snapshot date
    #filter list to current players only (those who have presence as of the last snapshot date)
    # else it gives an error for previous players. 
    # also write the data to activity_tracker_change table
    #ulog.logit(3, "Clearing activity change table and writing activity table to sql")
    #tbl_activity_tracker_change.delete()  #clear old change data in the activity_tracker_table
    #status = tbl_activity_tracker_change.write_to_sql(df_complete_player_growth.stack())  #status for success or failure
    #TODO to_sql fails with error "mysql exceptions unknown column '0' in 'field list' operationalerror 1054"
    
    # create a json file with player activity data that can be transmitted via django type web server to the android app
    # use the df with reset indexes with all data in columns. else the json file becomes unreadable
    jdata = df_complete_player_growth.to_json(orient='index') # write dataframe to json
    return jdata
    
    #improve table
    # 1. filter dates as needed
# DONE  #2. change title for the columns from raw date to Mar -1 type
    # 3. dynamically picking up dates for previous 6 months, last 4 weeks and this week days
    
''' # generateFortClusters
    # generate a dataframe containing player points growth for given players
    # Inputs : 
                1. 'playerIDs' :- list of playerIDs for castle combinations need to be created
                2. 'allianceIDs' :- if list given, does all playerIDs in the alliance
                3. 'max_fort_radius; : maximum radius to use for combination
                
    # Output:
                1. json formatted data in 'index' formatting
'''
def generateFortClusters(playerIDs, allianceIDs, max_fort_radius):

    global folderpath
    global lastUpdateDate
    global lastUpdated

    habitat_column_names = [COLUMN_ID, COLUMN_NAME, COLUMN_MAPX, COLUMN_MAPY, COLUMN_PLAYERID]
    
    df_player_habitat_data = pandas.DataFrame()
    # read player habitat data from sql
    if playerIDs.values():
        df_player_habitat_data = tbl_habitat.read_from_sql_to_dataframe(0, playerIDs.values())  # load selected rows fromt the  habitat table to a dataframe
        playerIDList = playerIDs.values()
    if allianceIDs.values():
        df_player_habitat_data = tbl_habitat.read_from_sql_to_dataframe_alliance(0, allianceIDs.values())  # load selected rows fromt the habitat table to a dataframe
        playerIDList = df_player_habitat_data.playerID.unique()
   # df_player_habitat_data.to_csv('player_habitat_comma.csv', encoding='utf-8')
    loop_counter = 0
    rowcount = playerIDList.size
    for playerID in playerIDList:
        utilities.show_progress(loop_counter, rowcount)  #show progress on screen
        df_player_castles = df_player_habitat_data[df_player_habitat_data[COLUMN_PLAYERID] == playerID]  #filter to current player
        
        #df_player_castles.to_csv('habitat_dump.csv', encoding='utf-8')    
        output_file_prefix = "US3_habitat_"
        #json_habitat_data_output = folderpath + lastUpdateDate + "/" + output_file_prefix + numpy.array_str(playerID) + ".json"
        json_habitat_data_output = lastUpdateDate + "/habitats/" + output_file_prefix + numpy.array_str(playerID) + ".json"
        jdata = df_player_castles.to_json(orient='index') # write dataframe to json
        saveFileToS3(jdata, json_habitat_data_output)  # write the habitat pairs into json file for future use
        
        output_file_prefix = "US3_cluster_"
        #json_fort_clusters_output_file = folderpath + lastUpdateDate + "/" + output_file_prefix + numpy.array_str(playerID) + ".json"
        json_fort_clusters_output_file = lastUpdateDate + "/clusters/" + output_file_prefix + numpy.array_str(playerID) + ".json"
        df_player_castles = df_player_castles[df_player_castles[COLUMN_PUBLICTYPE] == 0]  #filter to castles only
        df_player_castles = df_player_castles[habitat_column_names]  #pick only relevant columns
        distance_list = clusterizer.create_habitat_pairs(df_player_castles, max_fort_radius)
        jdata = pandas.DataFrame(distance_list).to_json(orient='index') # write dataframe to json 
        saveFileToS3(jdata, json_fort_clusters_output_file)  # write the habitat pairs into json file for future use
        loop_counter += 1


def saveFileToS3(jdata, filename):
    global bucket_name
    ulog.logit(2, "saveFileToS3... ")
    awsS3 = aws_s3.AwsS3(bucket_name)
    awsS3.writeDataToS3(filename, 'w+', jdata)
    #with open(filename, 'w+') as f:
     #   f.write(jdata)



    
    #replaced with habitat dump per player. can delete later in july 2016 if still not used by then
def create_habitat_dump(allianceIDs):
    df_all_habitats = tbl_habitat.read_from_sql_to_dataframe_alliance(0, allianceIDs.values())
    df_all_habitats.to_csv('habitat_dump.csv', encoding='utf-8')
    jdata = df_all_habitats.to_json(orient='index') # write dataframe to json
    saveFileToS3(jdata, json_habitat_data_output)  # write the habitat pairs into json file for future use
    
#main flow
def main():

    global folderpath
    global lastUpdateDate
    global lastUpdated
    ulog.logit(2, "Entering Main function.")
    
    alliance_legends = {"legends":26562}
    alliance_others = {'LoMB':10988,'KotLD':27080,'AoD':13308,'Venum':24144,'NewWorld':27071,'RoN':23301,'Forsaken':20143,
                        'RoE':395,'RoS':2920,'SS':198}  #'Legends':26562,
    alliance_all = {'none':0}
    playerIDs = {'Blade': 1373}
    utctime = datetime.utcnow()
    lastUpdated = utctime.strftime("%Y-%m-%d %H-%M-%S")
    lastUpdateDate = utctime.strftime("%Y-%m-%d")
    folderpath = os.getcwd() + "/" 
    make_sure_path_exists(folderpath + lastUpdateDate)
    tbl_habitat._tblname = dbhelper.TBL_HABITAT_US3
    

    max_fort_radius = 8
    #defining variables for alliances name/Ids to selectively filter data from database
    json_growth_tracker_output_file = 'activity_tracker.json'
    json_fort_clusters_output_file = 'fort_clusters.json'
    json_habitat_data_output = 'alliance_habitats_dump.json'
    allianceIDs = alliance_all  #new variable that can point to either alliance_legends or others as needed. 
                                    #Helps avoid changing all usages downstream
    
    #Start processing of Player Growht Tracker
    ulog.logit(3, "Running activity tracker: ")
    jdata = generatePlayerGrowthTracker(allianceIDs)
    saveFileToS3(jdata, lastUpdateDate + "/" + json_growth_tracker_output_file)  # write the habitat pairs into json file for future use
    ulog.logit(3, "Finishing player growth tracker.")
    

    #Start processing of habitat fort clusters
    #only get a list of combinations. because that looks like the most efficient way to transfer useful data through internet.
    #sending combinations by habitat id separately, and then separately updating castle data, no info is repeated and provides good 
    #flexibility to the android app for local manipulations like increased/decreased fort radius, combining with other player castles
    # that are short of the required number to make forts.
    ulog.logit(3, "Running clusterizer and habitat dump: ")
    generateFortClusters(playerIDs, allianceIDs, max_fort_radius)  #one of playerIDs and allianceIDs have the value, other is null
    ulog.logit(3, "Finishing habitat clustering process.")
    
    #create castle dump for the alliance and player selected
    #ulog.logit(3, "Running habitat dump: ")
    #create_habitat_dump(allianceIDs)  #one of playerIDs and allianceIDs have the value, other is null
    #ulog.logit(3, "Finishing habitat dump.")


if __name__ == "__main__":
  main()
