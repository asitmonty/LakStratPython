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
import datasourceHabitats
import datasourceAlliance
import datasourcePlayers
import utilities
import target_finder as clusterizer

global loglevel
global ulog
global tbl
global tbl_habitat
global tbl_alliance
global tbl_alliance_raw
global folderpath
global lastUpdated
global lastUpdateDate
global MAX_FORT_RADIUS
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
COLUMN_ALLIANCEID = "allianceID"
COLUMN_ALLIANCENAME = "allianceName"
COLUMN_ALLIANCERANK = "allianceRank"
COLUMN_NICK = "nick"
COLUMN_PLAYERPOINTS = "playerPoints"
COLUMN_PLAYERPOINTS_TOTAL = 'total'
COLUMN_ALLIANCE_ID_RAW = 'id'
COLUMN_ALLIANCE_NAME_RAW = 'name'
COLUMN_ALLIANCE_RANK_RAW = 'rank'


bucket_name='lakstrat'
CASTLE_MAX_POINTS = 290
MAX_FORT_RADIUS = 8

tbl_habitat = datasourceHabitats.TblHabitat()
tbl_player = datasourcePlayers.TblPlayer()
tbl_alliance = datasourceAlliance.TblAlliance()
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


def habitatChanges(include, world,):
    global MAX_FORT_RADIUS
    global folderpath
    global lastUpdateDate
    global lastUpdated

    tbl_habitat._tblname = 'tbl_habitat_alliance_change'
    habitat_column_names = [COLUMN_WORLD, COLUMN_ID, COLUMN_ALLIANCEID, COLUMN_LASTUPDATE_LNK]

    
    df_player_habitat_data = tbl_habitat.read_from_sql_to_dataframe_world(include, world)  # load selected rows fromt the habitat table to a dataframe
    df_player_habitat_data.to_csv('habitat_all.csv', encoding='utf-8')

    #df_player_habitat_data[COLUMN_ALLIANCEID].astype(float)
    df_pivot = df_player_habitat_data.pivot_table(index=COLUMN_ID, columns=COLUMN_LASTUPDATE_LNK, values=COLUMN_ALLIANCEID)
    column_names = df_pivot.columns.tolist()
    column_names = [str(x)[:10] for x in column_names]
    df_pivot.columns = column_names
    df_pivot.reset_index(level=0, inplace=True)
    counter = 0
    
    
    
    #df_unique_alliances.sort_values(date2, ascending = False)
    #df_unique_alliances = df_pivot.groupby(column_names).count()
    df_unique_alliances = pandas.DataFrame({'count' : df_pivot.groupby(column_names).id.count()}).reset_index()
    df_unique_alliances = df_unique_alliances[df_unique_alliances[df_unique_alliances.columns[0]] != df_unique_alliances[df_unique_alliances.columns[1]]]
    threshold = 30
    df_unique_alliances = df_unique_alliances[df_unique_alliances['count'] > threshold]
    df_unique_alliances = df_unique_alliances.sort_values('count', ascending = False)
    df_matrix = df_unique_alliances.pivot_table(index=column_names[1], columns=column_names[0], values='count')
    df_matrix = df_matrix.replace(np.nan,' ', regex=True)
    df_matrix.to_csv('alliance_matrix.csv', encoding='utf-8')
    #names = df_unique_alliances.columns.tolist()
    #names[0] = 'change'
    #df_unique_alliances.columns = names
    
    #df_unique_alliances = df_player_habitat_data[df_player_habitat_data['change'] > 1]
    #df_unique_alliances.to_csv('habitat_changes.csv', encoding='utf-8')
    

    # filter habitats that have at least one alliance change
    #print df_unique_alliances

    
def habitatChanges_players(include, world):
    global MAX_FORT_RADIUS
    global folderpath
    global lastUpdateDate
    global lastUpdated

    tbl_habitat._tblname = 'tbl_habitat_alliance_change'
    habitat_column_names = [COLUMN_WORLD, COLUMN_ID, COLUMN_PLAYERID, COLUMN_ALLIANCEID, COLUMN_LASTUPDATE_LNK]

    
    df_player_habitat_data = tbl_habitat.read_from_sql_to_dataframe_world(include, world)  # load selected rows fromt the habitat table to a dataframe
    df_player_habitat_data.to_csv('habitat_all.csv', encoding='utf-8')

    #df_player_habitat_data[COLUMN_ALLIANCEID].astype(float)
    df_pivot = df_player_habitat_data.pivot_table(index=COLUMN_ID, columns=COLUMN_LASTUPDATE_LNK, values=COLUMN_PLAYERID)
    column_names = df_pivot.columns.tolist()
    column_names = [str(x)[:10] for x in column_names]
    df_pivot.columns = column_names
    df_pivot.reset_index(level=0, inplace=True)
    counter = 0
    
    df_player_data = tbl_player.read_from_sql_to_dataframe_world(include, world)
    df_player_data.to_csv('player_map.csv', encoding='utf-8')
    import itertools
    list_id = df_player_data[COLUMN_ID].tolist()
    list_allianceId = df_player_data[COLUMN_ALLIANCEID].tolist()
    dict_player_data = dict(itertools.izip(list_id, list_allianceId))
    print df_pivot
    df_pivot[column_names[0]] = df_pivot[column_names[0]].apply(lambda x: dict_player_data.get(x))
    df_pivot[column_names[1]] = df_pivot[column_names[1]].apply(lambda x: dict_player_data.get(x))
    print df_pivot
    #df_unique_alliances.sort_values(date2, ascending = False)
    #df_unique_alliances = df_pivot.groupby(column_names).count()
    df_unique_alliances = pandas.DataFrame({'count' : df_pivot.groupby(column_names).id.count()}).reset_index()
    df_unique_alliances = df_unique_alliances[df_unique_alliances[df_unique_alliances.columns[0]] != df_unique_alliances[df_unique_alliances.columns[1]]]
    threshold = 10
    df_unique_alliances = df_unique_alliances[df_unique_alliances['count'] > threshold]
    df_unique_alliances = df_unique_alliances.sort_values('count', ascending = False)
    df_matrix = df_unique_alliances.pivot_table(index=column_names[1], columns=column_names[0], values='count')
    df_matrix = df_matrix.replace(np.nan,' ', regex=True)
    df_matrix.to_csv('alliance_matrix.csv', encoding='utf-8')


    '''
    # get habitat df with allianceId, lastupdate 
    # pivot lastupdate as column with habitatid as row, playerId as value
    # add column and put sum of distinct allianceIds 
    # filter rows to >1. this is all habitats that changed hands
    #
    # 
    # pivot habitat to rows and lastupdated to columns - get allianceid as values
    # move latest last updated to index
    # reorder alliance column move it in before habitat

    # 
    # matrix of alliances. separate file per world
    #       101 102 103
    # 101   46    0  23  # so 101 won 23 from 103 and (from 3rd row) lost 32 to 103
    # 102   48    0  3
    # 103   32    43 2  # 2 recycles

    '''
    


def saveFileToS3(jdata, filename):
    global bucket_name
    ulog.logit(2, "saveFileToS3... ")
    awsS3 = aws_s3.AwsS3(bucket_name)
    awsS3.writeDataToS3(filename, 'w+', jdata)
    #with open(filename, 'w+') as f:
     #   f.write(jdata)

def saveFileToLocal(jdata, filename):
    ulog.logit(2, "saveFileTolocal... ")
    with open(filename, 'w+') as f:
        f.write(jdata)

    
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


    habitatChanges_players(1, 'US-10')
    #habitatChanges(1, 'US-3')


    

    #Start processing of habitat fort clusters
    #only get a list of combinations. because that looks like the most efficient way to transfer useful data through internet.
    #sending combinations by habitat id separately, and then separately updating castle data, no info is repeated and provides good 
    #flexibility to the android app for local manipulations like increased/decreased fort radius, combining with other player castles
    # that are short of the required number to make forts.
    
    #create castle dump for the alliance and player selected
    #ulog.logit(3, "Running habitat dump: ")
    #create_habitat_dump(allianceIDs)  #one of playerIDs and allianceIDs have the value, other is null
    #ulog.logit(3, "Finishing habitat dump.")


if __name__ == "__main__":
  main()