#v1_02: working file for creating player growth tracker
#v1_03: Adding json creation for habitats cluster

import sqlite3
import pandas
import numpy as np
from datetime import datetime
import os
import aws_s3
import unicodedata  
import re
import json
import time
import sys


import databasehelper_mysql as dbhelper
import datasourceActivityTracker
import datasourceHabitats
import datasourceAlliance
import utilities
import target_finder as clusterizer

global loglevel
global ulog
global tbl
global tbl_habitat
global tbl_alliance_raw
global folderpath
global lastUpdated
global lastUpdateDate
global MAX_FORT_RADIUS
global bucket_name
global world_list

world_list = [u'US-3',u'US-11',u'US-10',u'US-9',u'US-8',u'US-7',u'US-6',u'US-5',u'US-4',u'US-2',u'US-1']
alliance_legends = {"legends":26562}
playerIDs = {'Blade': 1373}

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
COLUMN_ALLIANCEID = "allianceID"
COLUMN_ALLIANCENAME = "allianceName"
COLUMN_ALLIANCERANK = "allianceRank"
COLUMN_NICK = "nick"
COLUMN_PLAYERPOINTS = "playerPoints"
COLUMN_PLAYERPOINTS_TOTAL = 'total'
COLUMN_PLAYERPOINTS_TOTAL_CHECK = 'total_check'
COLUMN_ALLIANCE_ID_RAW = 'id'
COLUMN_ALLIANCE_NAME_RAW = 'name'
COLUMN_ALLIANCE_RANK_RAW = 'rank'


bucket_name='lakstrat'
CASTLE_MAX_POINTS = 290
MAX_FORT_RADIUS = 8

tbl_habitat = datasourceHabitats.TblHabitat()
tbl_alliance_raw = datasourceAlliance.TblAlliance()
tbl_activity_tracker = datasourceActivityTracker.TblActivityTracker()

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
    df_pivot = df.pivot_table(index=COLUMN_PLAYERID, columns=COLUMN_LASTUPDATE_LNK, values=COLUMN_PLAYERPOINTS) # pivot to create table with 
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
  size = len(column_names)
  first_date = column_names.pop(0)  #cut and store the last column name (latest date)
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
  df_diff.insert(0, COLUMN_PLAYERPOINTS_TOTAL, df[column_name_last])
  #df_diff[COLUMN_PLAYERPOINTS_TOTAL_CHECK] = df_diff.sum(axis = 1)
  df_diff = df_diff[np.isfinite(df[column_name_last])]  #filter to finiite values in the last date (assumption is if player dont exist on last, delta shows nan)
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
    player_colnames = [COLUMN_WORLD, COLUMN_ALLIANCERANK, COLUMN_ALLIANCENAME, COLUMN_ALLIANCEID, COLUMN_PLAYERID, COLUMN_NICK]  #list all columns to add to index (current index is only player id)
    # select the required columns from the first dataframe and drop duplicates
    df_details = df_player_data[player_colnames].drop_duplicates(COLUMN_PLAYERID, keep='last')
    # merge the details dataframe and the growth dataframe using playerId as common key
    df_merged_indexed = pandas.merge(df_details, df_growth, left_on=[COLUMN_PLAYERID], right_on=[COLUMN_PLAYERID])
    #filter the merged dataframe to exclude NaN values on latest snapshot
    row_count, column_count = df_merged_indexed.shape
    last_column_with_total = df_merged_indexed.columns.values[column_count - 1]  #pick the last collumn that has total playerpoints
    df_merged_indexed = df_merged_indexed.sort_values([COLUMN_ALLIANCEID,COLUMN_PLAYERPOINTS_TOTAL], ascending = [True, False])  #sort all values by current points as per the last snapshot date
    #filter list to current players only (those who have presence as of the last snapshot date)
    # else it gives an error for previous players. 
    df_merged_indexed = df_merged_indexed[np.isfinite(df_merged_indexed[last_column_with_total])]

    player_colnames_new = player_colnames + [COLUMN_PLAYERPOINTS_TOTAL]
    df_merged_indexed = df_merged_indexed.set_index(player_colnames_new)  # set the index by using selected columns
    df_merged_indexed.index.name = player_colnames_new  #set index name
    return df_merged_indexed
  

def generateAllianceList(allianceIDs):
    global world_list
        # get list of dates first (current date, last week, 5 last months)
        # eventually recreate the tracking table (drop the last reading if greater than the activity period) and add current
        
        # at present activity tracker table is created beforehand directly in SQL
        # TODO function to create/refresh/update the activity tracker table using tbl_player and tbl_alliance
        
        # read data from sql and create an activity table
    start_time = time.time()
    df_alliance_data = tbl_alliance_raw.read_from_sql_to_dataframe(0, allianceIDs.values())  # load selected rows fromt the player activity tracker table to a dataframe
    # create a json file with player activity data that can be transmitted via django type web server to the android app
    # use the df with reset indexes with all data in columns. else the json file becomes unreadable
    end_time = time.time()
    ulog.logit(3, "read alliance data: " + utilities.show_elapsed(start_time, end_time))
    #world_list = df_alliance_data.world.unique()
    dict_world_alliances = {}
    world_count = 0
    for world in world_list:
        start_time = time.time()
        df_alliances = df_alliance_data[df_alliance_data[COLUMN_WORLD] == world]  #filter to current alliance 
        max_alliance_id = df_alliances[COLUMN_ALLIANCE_ID_RAW].max()
        df_alliances_grouped = df_alliances[df_alliances[COLUMN_ALLIANCE_RANK_RAW] <= 100]
        import unicodedata
        unicodedata.normalize('NFKD', world).encode('ascii','ignore')
        world_mod = world
        import re
        world_mod = re.sub('[-]', '', world_mod)
        output_file_prefix = world_mod + "_alliances"
        json_alliance_data_output = lastUpdateDate + "/" + output_file_prefix + ".json"
        df_alliances_grouped = add_alliance(df_alliances_grouped, 99, 0, "others", 999, world, 0, 0)
        df_alliances_grouped = add_alliance(df_alliances_grouped, 100, -1, "<unaligned>", 1000, world, 0, 0)
        jdata = df_alliances_grouped.to_json(orient='index') # write dataframe to json
        saveFileToS3(jdata, json_alliance_data_output)  # save alliance group file to S3
        alliance_np = df_alliances_grouped.id.unique()
        dict_world_alliances[world] = alliance_np.tolist()
        generatePlayerGrowthTracker(world, alliance_np.tolist())
        world_count += 1
        end_time = time.time()
        ulog.logit(3, "World " + world + utilities.show_elapsed(start_time, end_time))
    end_time = time.time()
    ulog.logit(3, "time to locally process worlds... "+ utilities.show_elapsed(start_time, end_time))   
    alliance_count = 102 * world_count 
    ulog.logit(3, "Saving " + str(alliance_count) +" growth tracker files to S3... ")
    saveAllToS3(lastUpdateDate + "/alliances/")
    end_time = time.time()
    ulog.logit(3, "Total time to process growth tracker: " + utilities.show_elapsed(start_time, end_time))
    

# Asit 18 jun 2016 - repeating above code just to do clusterization since clusterization takes a long time.
#used to have it combined 
    #generateFortClusters(dict_world_alliances)  #one of playerIDs and allianceIDs have the value, other is null
    ulog.logit(3, "Running clusterizer and habitat dump: ")
    total_players = 0
    start_time = time.time()
    for world, alliance_list in dict_world_alliances.iteritems():
        ulog.logit(3, "World " + world)
        total_alliance_players = 0
        above100_player_count = 0
        start_time = time.time()
        df_alliance_data_below100 = tbl_habitat.read_from_sql_to_dataframe_alliance(1, world, alliance_list)  # load selected rows fromt the habitat table to a dataframe
        df_alliance_data_over100 = tbl_habitat.read_from_sql_to_dataframe_alliance(0, world, alliance_list)  # load selected rows fromt the habitat table to a dataframe
        total_alliance_players += df_alliance_data_below100.playerID.unique().size  # add player count in alliances with rank < 100
        above100_player_count += df_alliance_data_over100.playerID.unique().size  # add player count in alliances with rank > 100
        total_alliance_players += above100_player_count
        total_players += total_alliance_players  #add to running total for all world players
        player_count = 0
        alliance_count = len(alliance_list)
        for alliance_id in alliance_list:
            time_elapsed = time.time() - start_time
            utilities.show_progress_time(player_count, total_alliance_players, time_elapsed)  #show progress on screen
            df_alliance_habitat_data = df_alliance_data_below100[df_alliance_data_below100[COLUMN_ALLIANCEID] == alliance_id]
            player_count += df_alliance_habitat_data.playerID.unique().size
            if (not df_alliance_habitat_data.empty):
                generateFortClusters(world, df_alliance_habitat_data)  #one of playerIDs and allianceIDs have the value, other is null
        utilities.show_progress_time(player_count, total_alliance_players, time_elapsed)  #show progress on screen
        generateFortClusters(world, df_alliance_data_over100)  #one of playerIDs and allianceIDs have the value, other is null
        utilities.clear_progress_bar()  #show progress on screen
        end_time = time.time()
        ulog.logit(3, str(total_alliance_players) + " players" + utilities.show_elapsed(start_time, end_time))
    end_time = time.time()
    ulog.logit(3, "\nall habitat and cluster local " +  utilities.show_elapsed(start_time, end_time))

    start_time = time.time()
    saveAllToS3(lastUpdateDate + "/clusters/")
    end_time = time.time()
    ulog.logit(3, "write " + str(total_players) + " cluster files to s3 " +  utilities.show_elapsed(start_time, end_time))
    start_time = time.time()
    saveAllToS3(lastUpdateDate + "/habitats/")
    end_time = time.time()
    ulog.logit(3, "write " + str(total_players) + " habitat files to s3 " +  utilities.show_elapsed(start_time, end_time))

    ulog.logit(3, "Finishing habitat clustering process.")
    


def add_alliance(df_alliances_grouped, row, _id, name, rank, world, lastupdate, lastupdate_lnk ):
    new_row = {}
    new_row[COLUMN_WORLD] = world
    new_row[COLUMN_LASTUPDATED] = lastupdate_lnk
    new_row[COLUMN_LASTUPDATE_LNK] = lastupdate
    new_row[COLUMN_ALLIANCE_ID_RAW] = _id
    new_row[COLUMN_ALLIANCE_NAME_RAW] = name
    new_row[COLUMN_ALLIANCE_RANK_RAW] = rank
    df_alliances_grouped = df_alliances_grouped.append(new_row, ignore_index = True)
    return df_alliances_grouped

    #improve table
    # 1. filter dates as needed
# DONE  #2. change title for the columns from raw date to Mar -1 type
    # 3. dynamically picking up dates for previous 6 months, last 4 weeks and this week days


''' # generatePlayerGrowthTracker
    # generate a dataframe containing player points growth for given players
    # Inputs : 
                1. 'df' :- list of alliances for which player growth needs to be calculated
                
    # Output:
                1. json formatted data in 'index' formatting
'''
def generatePlayerGrowthTracker(world, allianceIDs):
    # get list of dates first (current date, last week, 5 last months)
    # eventually recreate the tracking table (drop the last reading if greater than the activity period) and add current
    
    # at present activity tracker table is created beforehand directly in SQL
    # TODO function to create/refresh/update the activity tracker table using tbl_player and tbl_alliance
    ulog.logit(2, "Processing player growth tracker for world " + world)    
    # read data from sql and create an activity table
    world_mod = world
    world_mod = re.sub('[-]', '', world_mod)
    all_alliances = [0]
    df_player_data = tbl_activity_tracker.read_from_sql_to_dataframe(-1, world, all_alliances)  # load selected rows fromt the player activity tracker table to a dataframe
    df_player_data.to_csv('all_alliances.csv', encoding='utf-8')
    df_player_points = get_pivoted_dates_table(df_player_data)  #get pivoted dataframe with dates as columns and playerIds as rows
    df_player_growth = get_player_growth_table(df_player_points)  #rework the pivoted dataframe to get points growth instead of raw points
    df_complete_player_growth = add_player_details_to_index(df_player_growth, df_player_data)  # reset indexes to get a spreadsheet style table with full details in rows
    # write the spreadsheet type tables to csv files for easy validation/use where json from next step cannot be used
    ulog.logit(2, "saving file to csv ")
    player_growth_csv_filename = 'player_growth' + world + '.csv'
    df_complete_player_growth.to_csv(player_growth_csv_filename, encoding='utf-8')
    
    #TODO for some reason reset_index doesnt default the column names from index names. so getting index_names, column_names and renaming the columns after reset_index. else could have used reset_index directly in pandas.melt
    index_names = df_complete_player_growth.index.name
    column_names = list(df_complete_player_growth.columns.values)
    df_complete_player_growth = df_complete_player_growth.reset_index()
    df_complete_player_growth.columns = index_names + column_names
    df_complete_player_growth = pandas.melt(df_complete_player_growth, id_vars=index_names, value_vars=column_names, var_name=COLUMN_LASTUPDATE_LNK, value_name=COLUMN_PLAYERPOINTS)
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

    #allianceId_list = df_complete_player_growth.allianceId.unique()
    loop_counter = 0
    rowcount = len(allianceIDs)
    start_time = time.time()
    folderpath = os.getcwd() + "/"
    make_sure_path_exists(folderpath + lastUpdateDate + "/alliances/")
    unicodedata.normalize('NFKD', world_mod).encode('ascii','ignore')
    max_alliance_id = max(allianceIDs)  #unaligned list

    for alliance_id in allianceIDs:
        ulog.logit(1, "processing alliance " + str(alliance_id))
        output_file_prefix = world_mod + "_alliance_"
        #json_habitat_data_output = folderpath + lastUpdateDate + "/" + output_file_prefix + np.array_str(playerID) + ".json"
        if (alliance_id > 0):
            df_alliance_growth = df_complete_player_growth[df_complete_player_growth[COLUMN_ALLIANCEID] == alliance_id]  #filter to current alliance 
            json_alliance_data_output = lastUpdateDate + "/alliances/" + output_file_prefix + str(long(alliance_id)) + ".json"
        elif (alliance_id == -1):
            df_alliance_growth = df_complete_player_growth[df_complete_player_growth[COLUMN_ALLIANCEID].isnull()]  #filter to current alliance 
            json_alliance_data_output = lastUpdateDate + "/alliances/" + output_file_prefix + "none" + ".json"
        if not df_alliance_growth.empty:
            jdata = df_alliance_growth.to_json(orient='index') # write dataframe to json
            filename = folderpath + json_alliance_data_output
            saveToLocal(jdata, filename, lastUpdateDate) 
        loop_counter += 1
    ulog.logit(2, "completed creating local alliance files ")
    end_time = time.time()
    ulog.logit(2, "processing 'other' alliances")
    df_alliance_growth = df_complete_player_growth[~df_complete_player_growth[COLUMN_ALLIANCEID].isin(allianceIDs)]  #filter to current alliance 
    df_alliance_growth = df_alliance_growth[~df_alliance_growth[COLUMN_ALLIANCEID].isnull()]  #filter to current alliance 
    output_file_prefix = world_mod + "_alliance_"
    #json_habitat_data_output = folderpath + lastUpdateDate + "/" + output_file_prefix + np.array_str(playerID) + ".json"
    json_alliance_data_output = lastUpdateDate + "/alliances/" + output_file_prefix + "0" + ".json"
    jdata = df_alliance_growth.to_json(orient='index') # write dataframe to json
    saveToLocal(jdata, json_alliance_data_output, lastUpdateDate) 
    ulog.logit(1, "completed processing growth trackers for world " +  world)
    #saveFileToS3(jdata, json_alliance_data_output)  # write the habitat pairs into json file for future use

    
''' # generateFortClusters
    # generate a dataframe containing player points growth for given players
    # Inputs : 
                1. 'playerIDs' :- list of playerIDs for castle combinations need to be created
                2. 'allianceIDs' :- if list given, does all playerIDs in the alliance
                
                
    # Output:
                1. json formatted data in 'index' formatting
'''
def generateFortClusters(world, df_player_habitat_data):
    global MAX_FORT_RADIUS
    global folderpath
    global lastUpdateDate

    ulog.logit(2, "processing fort clusters")
    habitat_column_names = [COLUMN_WORLD, COLUMN_ID, COLUMN_NAME, COLUMN_MAPX, COLUMN_MAPY, COLUMN_PLAYERID]
    playerIDList = df_player_habitat_data.playerID.unique()
    loop_counter = 0
    rowcount = playerIDList.size
    world_mod = world
    world_mod = re.sub('[-]', '', world_mod)
    output_file_prefix = world_mod + "_alliances"

    folderpath = os.getcwd() + "/"
    make_sure_path_exists(folderpath + lastUpdateDate + "/habitats/")
    make_sure_path_exists(folderpath + lastUpdateDate + "/clusters/")
    unicodedata.normalize('NFKD', world_mod).encode('ascii','ignore')

    for playerID in playerIDList:
        ulog.logit(1, "processing player " + str(playerID))
        ulog.logit(1, "processing habitat")
        df_player_castles = df_player_habitat_data[df_player_habitat_data[COLUMN_PLAYERID] == playerID]  #filter to current player
        #df_player_castles.to_csv('habitat_dump.csv', encoding='utf-8')    
        output_file_prefix = world_mod + "_habitat_"
        #json_habitat_data_output = folderpath + lastUpdateDate + "/" + output_file_prefix + np.array_str(playerID) + ".json"
        playerID_str = np.array_str(playerID, precision=0)
        json_habitat_data_output = 
            lastUpdateDate + "/habitats/" + output_file_prefix + playerID_str + ".json"
        if not df_player_castles.empty:
            jdata = df_player_castles.to_json(orient='index') # write dataframe to json
            filename = folderpath + json_habitat_data_output
            saveToLocal(jdata, filename, lastUpdateDate) 
        #saveFileToS3(jdata, json_habitat_data_output)  # write the habitat pairs into json file for future use
        ulog.logit(1, "processing cluster")
        output_file_prefix = world_mod + "_cluster_"
        #json_fort_clusters_output_file = folderpath + lastUpdateDate + "/" + output_file_prefix + np.array_str(playerID) + ".json"
        json_fort_clusters_output_file = lastUpdateDate + "/clusters/" + output_file_prefix + playerID_str + ".json"
        df_player_castles = df_player_castles[df_player_castles[COLUMN_PUBLICTYPE] == 0]  #filter to castles only
        df_player_castles = df_player_castles[habitat_column_names]  #pick only relevant columns
        ulog.logit(1, "creating habitat pairs")
        distance_list = clusterizer.create_habitat_pairs(df_player_castles, MAX_FORT_RADIUS, world)
        if distance_list:
            jdata = pandas.DataFrame(distance_list).to_json(orient='index') # write dataframe to json 
            filename = folderpath + json_fort_clusters_output_file
            saveToLocal(jdata, filename, lastUpdateDate) 
            #saveFileToS3(jdata, json_fort_clusters_output_file)  # write the habitat pairs into json file for future use
        ulog.logit(1, "completed player " + str(playerID))



def saveToLocal(jdata, filename, lastUpdateDate):
    with open(str(filename), 'w+') as f:
        f.write(jdata)


def saveFileToS3(jdata, filename):
    ulog.logit(2, "saveFileToS3... ")
    awsS3 = aws_s3.AwsS3(bucket_name)
    awsS3.writeDataToS3(filename, 'w+', jdata)
    #with open(filename, 'w+') as f:
     #   f.write(jdata)

def saveAllToS3(key_path):
    folderpath = os.getcwd() + "/"
    overwrite = True
    ulog.logit(2, "saveFileToS3... ")
    awsS3 = aws_s3.AwsS3(bucket_name)
    awsS3.writeFolderToS3(key_path, folderpath, overwrite)
    #with open(filename, 'w+') as f:
     #   f.write(jdata)


def saveAllToS3SkipExisting(key_path, overwrite):
    folderpath = os.getcwd() + "/"
    ulog.logit(2, "saveFileToS3... ")
    awsS3 = aws_s3.AwsS3(bucket_name)
    awsS3.writeFolderToS3(key_path, folderpath, overwrite)
    
    
    #replaced with habitat dump per player. can delete later in july 2016 if still not used by then
def create_habitat_dump(allianceIDs):
    df_all_habitats = tbl_habitat.read_from_sql_to_dataframe_alliance(0, allianceIDs.values())
    df_all_habitats.to_csv('habitat_dump.csv', encoding='utf-8')
    jdata = df_all_habitats.to_json(orient='index') # write dataframe to json
    saveFileToS3(jdata, json_habitat_data_output)  # write the habitat pairs into json file for future use

    
def update_s3(lastUpdateDate, overwrite):
    # start_time = time.time()
    # saveAllToS3("2016-07-29" + "/alliances/")
    # end_time = time.time()
    # ulog.logit(3, "write clusters to s3: " +  utilities.show_elapsed(start_time, end_time))
    ulog.logit(3, "writing cluster files to s3:")
    start_time = time.time()
    saveAllToS3SkipExisting(lastUpdateDate + "/clusters/", overwrite)
    end_time = time.time()
    ulog.logit(3, "write cluster files to s3" +  utilities.show_elapsed(start_time, end_time))
    ulog.logit(3, "writing habitat files to s3:")
    start_time = time.time()
    saveAllToS3SkipExisting(lastUpdateDate + "/habitats/", overwrite)
    end_time = time.time()
    ulog.logit(3, "write habitat files to s3" +  utilities.show_elapsed(start_time, end_time))


#main flow
def main():

    global folderpath
    global lastUpdateDate
    global lastUpdated
    ulog.logit(3, "Data processing module...")
    ulog.logit(2, "Entering Main function.")
    
    utctime = datetime.utcnow()
    lastUpdated = utctime.strftime("%Y-%m-%d %H-%M-%S")

    #TODO change last update date to that from mysql table, not today's date
    lastUpdateDate = utctime.strftime("%Y-%m-%d")
    folderpath = os.getcwd() + "/" 
    make_sure_path_exists(folderpath + lastUpdateDate)
    
    #Start processing of Player Growht Tracker
    ulog.logit(3, "Running activity tracker: ")
    alliance_all = {'none':0}
#    generateAllianceList(alliance_all)
    ulog.logit(3, "Finishing player growth tracker.")
    
    lastUpdateDate = "2016-08-01"
    update_s3(lastUpdateDate, False)




if __name__ == "__main__":
  main()