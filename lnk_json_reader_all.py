#v1_0: add sqlite functions and class
#handle missing columns
#return rows added with every run. 
#return errors
# error check for bad connection, couldnt connect tod b, write failure, in general mysql data handling


import json
import sqlite3
import time
import urllib2
import itertools
from itertools import islice, chain

import sys
import os

import datasourceAlliance
import datasourcePlayers
import datasourceHabitats
#import aws_s3
import utilities

from time import gmtime, strftime
from datetime import datetime


global tblAlliance
global tblPlayer
global tblHabitat
global loglevel
global ulog
global lastUpdateLnk
global lastUpdated
global lastUpdateDate

global bucket

global folderpath

if len(sys.argv) == 2:
  logLevel = int(sys.argv[1])
else:
  logLevel = 3

ulog = utilities.utilities(logLevel)

tblAlliance = datasourceAlliance.TblAlliance()
tblPlayer = datasourcePlayers.TblPlayer()
tblHabitat = datasourceHabitats.TblHabitat()

JSON_FILE = "lnk_data.json"
URL_FORMAT = "http://public-data.lordsandknights.com/LKWorldServer-"

# set remote and local file location
filename = 'lnk_data.zip'
folderpath = os.getcwd() + "/"

bucket_name='app.monty.lnk'
local_file_path = "/Users/asitm/Documents/Workspace/Projects/LakStrat/python scripts/" + JSON_FILE

#download the url file after checking connection for mysql writing
''' OLD CODE REPLACING WITH THE DOWNLOAD FUNCTION BELOW
def download(world, fileType, filename):
    url = URL_FORMAT + world + "/" +fileType + ".json.gz"
    ulog.logit(3, "url - " + url)

    webdata = read_URL(url)
    open(filename,'wb').write(webdata)
    unzip(filename)
'''
#download for s3 and local copy
def download(world, fileType, updateType):

    global lastUpdateLnk
    global lastUpdated
    global lastUpdateDate
    saveAsFileName = world + '_' + lastUpdated + '_' + fileType + '_' + updateType
    url = URL_FORMAT + world + "/" + fileType + updateType + ".json.gz"

    ulog.logit(3, "url - " + url)
    webdata = read_URL(url)
    savedFileName = folderpath + lastUpdateDate + "/" + saveAsFileName + ".json"
    zipFileName = savedFileName + ".gz"
    make_sure_path_exists(folderpath + lastUpdateDate)

    open(zipFileName,'wb+').write(webdata)
    unzip(zipFileName)
    return savedFileName

def make_sure_path_exists(path):
    import os
    import errno

    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise
    

#download the url file after checking connection
def read_URL(url):
    import contextlib
    with contextlib.closing(urllib2.urlopen(url)) as webUrl:
    #get the result code and print it
        if (webUrl.getcode() == 200):
            data = webUrl.read()
            return data
        else:
            ulog.logit(3, "could not connect to URL - " + url)
            ulog.logit(2, "Error code " + str(webUrl.getcode()))

#unzip the .gz file and save it locally for extraction of JSON
def unzip(source_filename):
    import zipfile,os.path, gzip
    with gzip.open(source_filename, 'rb') as f:
      file_content = f.read()

    f = open(JSON_FILE, 'w+')
    f.write(file_content)
    f.close

#calculate number of lines int he file
def file_len(fname):
    with open(fname) as f:
        #for i, l in enumerate(f):
         #   pass
        count = sum(1 for _ in f)
    return count
    
def passToDatasource(list_jdata, fileType):
    start_time = time.time()

    if fileType == "alliances" : 
        tblAlliance.open_conn()
        tblAlliance.insert_multiple_to_table(list_jdata)
        tblAlliance.close()
    if fileType == "players" : 
        tblPlayer.open_conn()
        tblPlayer.insert_multiple_to_table(list_jdata)
        tblPlayer.close()
    if fileType == "habitats" : 
        tblHabitat.open_conn()
        tblHabitat.insert_multiple_to_table(list_jdata)
        tblHabitat.close()
    end_time = time.time()
    #ulog.logit(3, "time to write one row: " + utilities.show_elapsed(start_time, end_time))
#For saving s3 and local copy 
def getFileFromUrl(fileType, world, updateType):

    global lastUpdateLnk
    global lastUpdated
    global lastUpdateDate
    #get the file from url
    if updateType == "last4h":
      savedFileName = download(world, fileType, "-last4h")
    else:
      savedFileName = download(world, fileType, "")
    return savedFileName

def save2s3(fileType, updateType, file_key_prefix, savedFileName):
    success = 0
    # ulog.logit(2, "Entering Function save2s3")
    # file_key = file_key_prefix + '_' + fileType + '_' + updateType + ".json.gz"
    # ulog.logit(2, "Saving file '" + file_key + "' to s3 bucket '" + bucket_name + "'")
    # success = aws_s3.connectAndSave2s3(bucket_name, file_key, savedFileName)
    # return success
      
    
def process(fileType, world, updateType):

    global lastUpdateLnk
    global lastUpdated
    global lastUpdateDate
    
    start_time = time.time()
    ulog.logit(2, "Entering function - process().")
    ''' REPLACED WITH DIRECT PASSING OF UPDATETYPE TO THE FUNCTION GETFILEFROMURL
    if updateType == "last4h":
      download(world, fileType + "-last4h", filename)
    else:
      download(world, fileType, filename)'''
    #for downloading the json file
    savedFileName = getFileFromUrl(fileType, world, updateType)
    
    # for creating local and aws copies
    #For aws file keys
    file_key_prefix = lastUpdateDate.strip(' ') + '/' + world + '_' + lastUpdated
    result = save2s3(fileType, updateType, file_key_prefix, savedFileName)
    end_time = time.time()
    ulog.logit(3, "download file and save to local" + utilities.show_elapsed(start_time, end_time))
    total_lines = file_len(JSON_FILE)
    ulog.logit(3, "total lines in data file " + str(total_lines))
    linecounter = 0
    block_counter = 0

    start_time_block = time.time()
    list_jdata = []
    with open(JSON_FILE, "rb") as file:
        block = ""
        for line in file:
          linecounter += 1
          #show_progress(JSON_FILE, linecounter, total_lines)  #commented for log files
          if ("{" in line) and (":" not in line) and ("[" in line):
            block += "{"
          elif ("}" in line) and (":" not in line):
            block += "}"
            block = utilities.escape_sql_quotes(block)
            jfile = json.loads(block)
            jfile['lastSnapshot'] = lastUpdated
            jfile['lastUpdateLnk'] = lastUpdateLnk
            jfile['world'] = world
            if jfile['points'] > 40: list_jdata.append(jfile)
            block_counter += 1
            if block_counter == 200000:
                passToDatasource(list_jdata, fileType)
                list_jdata[:] = []
                block_counter = 0
            #passToDatasource(jfile, fileType)
            block = "{"
          else:
            block += unicode(line)

        print "\n"
    passToDatasource(list_jdata, fileType)
    end_time = time.time()
    ulog.logit(3, "time to process json: " + utilities.show_elapsed(start_time_block, end_time))
    ulog.logit(1, "Exiting function - process().")


def process_all_worlds():
    with open(folderpath + 'worlds.txt', "rb") as worlds:
        for world in worlds:
            ulog.logit(3, "Processing world " + world)
            world = world.rstrip('\r\n')
            
            url_lastupdated = "http://public-data.lordsandknights.com/LKWorldServer-" + world + "/lastUpdate"
            #lastUpdated = read_URL(url_lastupdated)
            utctime = datetime.utcnow()
            lastUpdated = utctime.strftime("%Y-%m-%d %H-%M-%S")
            lastUpdateDate = utctime.strftime("%Y-%m-%d")
            ulog.logit(3, "update timestamp for this run " + lastUpdated)

            updateType = "full"

            ulog.logit(3, "update type '" + updateType + "'")
            ulog.logit(3, "Beginning Data Pull...")

            process("players", world, updateType)

#main flow
def main():

        global lastUpdateLnk
        global lastUpdated
        global lastUpdateDate
        start_time = time.time()
        end_time = time.time()
        utilities.show_elapsed(start_time, end_time)

        ulog.logit(3, "Starting update from LnK data dump: ")
        ulog.logit(2, "Entering Main function.")


        #Process the three files for US-3 and US-11
        for world in ['US-3', 'US-11', 'US-1', 'US-2', 'US-4', 'US-5', 'US-6', 'US-7', 'US-8', 'US-9', 'US-10']:
            ulog.logit(3, "Processing world " + world)
            world = world.rstrip('\r\n')
            
            url_lastupdated = "http://public-data.lordsandknights.com/LKWorldServer-" + world + "/lastUpdate"
            #TODO - get timestamp from the url (or current system time if url reflects time that has a delta >4 hours to current time)
            #TODO - add the check for updated time >4 hours from current

            lastUpdateLnk = read_URL(url_lastupdated)
            dateSize = len(lastUpdateLnk)
            lastUpdateLnk =  lastUpdateLnk[:10] + ' ' + lastUpdateLnk[11:dateSize-1];
            utctime = datetime.utcnow()
            lastUpdated = utctime.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            lastUpdateDate = utctime.strftime("%Y-%m-%d")
            ulog.logit(3, "lnk lastupdate timestamp for this run " + lastUpdateLnk)
            ulog.logit(3, "update timestamp for this run " + lastUpdated)
            
            #updateType = "last4h"
            updateType = "full"
            
            #For adding to mysql database
            ulog.logit(3, "update type '" + updateType + "'")
            ulog.logit(3, "Beginning Data Pull...")
            process("alliances", world, updateType)
            process("players", world, updateType)
            process("habitats", world, updateType)
            ulog.logit(3, "Completed world: " + world)

        process_all_worlds()


if __name__ == "__main__":
  main()