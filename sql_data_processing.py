import sqlite3
import datasourceAlliance
import datasourcePlayers
import datasourceHabitats
#import aws_s3
import utilities


global tblAlliance
global tblPlayer
global tblHabitat
global loglevel
global ulog


if len(sys.argv) == 2:
  logLevel = int(sys.argv[1])
else:
  logLevel = 3

ulog = utilities.utilities(logLevel)

tblAlliance = datasourceAlliance.TblAlliance()
tblPlayer = datasourcePlayers.TblPlayer()
tblHabitat = datasourceHabitats.TblHabitat()



def main():

    global lastUpdateLnk
    # get last months
    # except the 1st date available, move everything else to archive
    # keep the last 4 months, and move everythin else to archive
    
    #get this month
    # if it is 1st 
    #   move all remaining from last month except 1st (like 30th, 31st) to archie
    #   move all 1st that are older than 5 months to archive
    # if it is 16th, 2nd
    #   dont move anything to archive
    # if it is 15th
    #   move last month 15th to archive



if __name__ == "__main__":
  main()