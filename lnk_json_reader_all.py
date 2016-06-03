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

import databasehelper
import datasourceAlliance
import datasourcePlayers
import datasourceHabitats
import aws_s3
import utilities

from time import gmtime, strftime
from datetime import datetime


global tblAlliance
global tblPlayer
global tblHabitat
global loglevel
global ulog

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
folderpath = os.getcwd() + "\\"

bucket_name='app.monty.lnk'
local_file_path = "C:\\cygwin64\\home\\nellie\\lnk\\" + JSON_FILE

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
def download(world, lastUpdateDate, lastUpdated, fileType, updateType):

    saveAsFileName = world + '_' + lastUpdated + '_' + fileType + '_' + updateType
    url = URL_FORMAT + world + "/" + fileType + updateType + ".json.gz"

    ulog.logit(3, "url - " + url)
    webdata = read_URL(url)
    savedFileName = folderpath + lastUpdateDate + "\\" + saveAsFileName + ".json"
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
    
def passToDatasource(jfile, fileType):

    if fileType == "alliances" and jfile['points'] > 40: tblAlliance.writeToTable(jfile)
    if fileType == "players" and jfile['points'] > 40: tblPlayer.writeToTable(jfile)
    if fileType == "habitats" and jfile['points'] > 40: tblHabitat.writeToTable(jfile)

#For saving s3 and local copy 
def getFileFromUrl(fileType, world, lastUpdateDate, lastUpdated, updateType):

    #get the file from url
    if updateType == "last4h":
      savedFileName = download(world, lastUpdateDate, lastUpdated, fileType, "-last4h")
    else:
      savedFileName = download(world, lastUpdateDate, lastUpdated, fileType, "")
    return savedFileName

def save2s3(fileType, updateType, file_key_prefix, savedFileName):
    success = 0
    ulog.logit(2, "Entering Function save2s3")
    file_key = file_key_prefix + '_' + fileType + '_' + updateType + ".json.gz"
    ulog.logit(2, "Saving file '" + file_key + "' to s3 bucket '" + bucket_name + "'")
    success = aws_s3.connectAndSave2s3(bucket_name, file_key, savedFileName)
    return success
      
    
def process(fileType, world, lastUpdated, lastUpdateDate, updateType):

    ulog.logit(2, "Entering function - process().")
    ''' REPLACED WITH DIRECT PASSING OF UPDATETYPE TO THE FUNCTION GETFILEFROMURL
    if updateType == "last4h":
      download(world, fileType + "-last4h", filename)
    else:
      download(world, fileType, filename)'''
    #for downloading the json file
    savedFileName = getFileFromUrl(fileType, world, lastUpdateDate, lastUpdated, updateType)
    
    # for creating local and aws copies
    #For aws file keys
    file_key_prefix = lastUpdateDate.strip(' ') + '/' + world + '_' + lastUpdated
    result = save2s3(fileType, updateType, file_key_prefix, savedFileName)

    total_lines = file_len(JSON_FILE)
    ulog.logit(3, "total lines in data file " + str(total_lines))
    linecounter = 0

    with open(JSON_FILE, "rb") as file:
        block = ""
        for line in file:
          linecounter += 1
          #show_progress(JSON_FILE, linecounter, total_lines)  #commented for log files
          if ("{" in line) and (":" not in line) and ("[" in line):
            block += "{"
          elif ("}" in line) and (":" not in line):
            block += "}"
            jfile = json.loads(block)
            jfile['lastUpdated'] = lastUpdated
            jfile['world'] = world
            passToDatasource(jfile, fileType)
            block = "{"
          else:
            block += line
        print "\n"
    ulog.logit(1, "Exiting function - process().")


#main flow
def main():

        ulog.logit(3, "Starting update from LnK data dump: ")
        ulog.logit(2, "Entering Main function.")

        #Process the three files for US-3 and US-11
        for world in ['US-3', 'US-11']:
            ulog.logit(3, "Processing world " + world)
            world = world.rstrip('\r\n')
            
            url_lastupdated = "http://public-data.lordsandknights.com/LKWorldServer-" + world + "/lastUpdate"
            #TODO - get timestamp from the url (or current system time if url reflects time that has a delta >4 hours to current time)
            #TODO - add the check for updated time >4 hours from current
            #lastUpdated = read_URL(url_lastupdated)
            utctime = datetime.utcnow()
            lastUpdated = utctime.strftime("%Y-%m-%d %H-%M-%S")
            lastUpdateDate = utctime.strftime("%Y-%m-%d")
            ulog.logit(3, "update timestamp for this run " + lastUpdated)
            
            #updateType = "last4h"
            updateType = "full"
            
            #For adding to mysql database
            ulog.logit(3, "update type '" + updateType + "'")
            ulog.logit(3, "Beginning Data Pull...")
            process("alliances", world, lastUpdated, lastUpdateDate, updateType)
            process("players", world, lastUpdated, lastUpdateDate, updateType)
            process("habitats", world, lastUpdated, lastUpdateDate, updateType)
            ulog.logit(3, "Completed world: " + world)


            
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

                process("players", world, lastUpdated, lastUpdateDate, updateType)

if __name__ == "__main__":
  main()