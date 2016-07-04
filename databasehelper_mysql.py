          #database class
import pymysql as MySQLdb

import sys

class DbHelper:

    global SQLITE_DB
    global TBL_ALLIANCE
    global TBL_PLAYER
    global TBL_HABITAT
    global TBL_ALLIANCE_RAW
    global TBL_PLAYER_RAW
    global TBL_HABITAT_RAW
    global TBL_HABITAT_US3
    global TBL_PLAYER_ALL
    global TBL_ACTIVITY_TRACKER
    global TBL_US3_ACTIVITY_TRACKER_CHANGE
    global SQL_CREATE_ALLIANCE_TABLE
    global SQL_CREATE_PLAYER_TABLE
    global SQL_CREATE_HABITAT_TABLE

    global host
    global passwd
    global user
    global dbname
    global port

    #host = "localhost"
    #user = "root"
    host = "west2-mysql-instance1.ctrzwyr54voz.us-west-2.rds.amazonaws.com"
    port = 3306
    user = "awsuser"
    passwd = "nellierova"
    dbname = "db_lakstrat"

    TBL_ALLIANCE = "tbl_alliance"
    TBL_PLAYER = "tbl_player"
    TBL_HABITAT = "tbl_habitat"
    TBL_ALLIANCE_RAW = "tbl_alliance_raw"
    TBL_PLAYER_RAW = "tbl_player_raw"
    TBL_HABITAT_RAW = "tbl_habitat_raw"

    TBL_HABITAT_US3 = "tbl_habitat_us3"
    TBL_PLAYER_ALL = "tbl_player_all"
    TBL_ACTIVITY_TRACKER = "tbl_activity_tracker"
    TBL_US3_ACTIVITY_TRACKER_CHANGE = "tbl_us3_activity_tracker"


    
    
    COLUMN_PRIMARY_KEY = "primary_key"
    COLUMN_ID = "id"
    COLUMN_NAME = "name"
    COLUMN_DESC = "description"
    COLUMN_PLAYERIDS = "playerIDs"
    COLUMN_RANKAVERAGE = "rankAverage"
    COLUMN_RANK = "rank"
    COLUMN_POINTSAVERAGE = "pointsAverage"
    COLUMN_POINTS = "points"

    COLUMN_NICK = "nick"
    COLUMN_HABITATIDS = "habitatIDs"
    COLUMN_ALLIANCEID = "allianceID"
    COLUMN_ALLIANCEPERM = "alliancePermission"
    COLUMN_UNDERATTACKPROT = "underAttackProtection"
    COLUMN_ONVACATION = "onVacation"

    COLUMN_MAPX = "mapX"
    COLUMN_MAPY = "mapY"
    COLUMN_CREATIONDATE = "creationDate"
    COLUMN_PLAYERID = "playerID"
    COLUMN_PUBLICTYPE = "publicType"
    COLUMN_WORLD = "world"
    COLUMN_LASTUPDATE_LNK = "lastUpdateLnk"
    COLUMN_LASTUPDATED = "lastSnapshot"
    
    
    SQL_CREATE_ALLIANCE_TABLE = ("create table "
          + TBL_ALLIANCE + " ("
          + COLUMN_PRIMARY_KEY + " INTEGER PRIMARY KEY AUTO_INCREMENT,"
          + COLUMN_LASTUPDATED + " text,"
          + COLUMN_WORLD + " text,"
          + COLUMN_ID + " integer,"
          + COLUMN_NAME + " text,"
          + COLUMN_DESC + " text,"
          + COLUMN_PLAYERIDS + " text,"
          + COLUMN_RANKAVERAGE + " integer,"
          + COLUMN_RANK + " integer,"
          + COLUMN_POINTSAVERAGE + " integer,"
          + COLUMN_POINTS + " integer"
          + ")")

    SQL_CREATE_PLAYER_TABLE = ("create table "
          + TBL_PLAYER + " ("
          + COLUMN_PRIMARY_KEY + " INTEGER PRIMARY KEY AUTO_INCREMENT,"
          + COLUMN_LASTUPDATED + " text,"
          + COLUMN_WORLD + " text,"
          + COLUMN_ID + " integer,"
          + COLUMN_NICK + " text,"
          + COLUMN_HABITATIDS + " text,"
          + COLUMN_ALLIANCEID + " integer,"
          + COLUMN_ALLIANCEPERM + " integer,"
          + COLUMN_POINTS + " integer,"
          + COLUMN_RANK + " integer,"
          + COLUMN_UNDERATTACKPROT + " integer,"  #use integer 0 1 to store this boolean value later
          + COLUMN_ONVACATION + " integer" #use integer 0 1 to store this boolean value later
          + ")")

    SQL_CREATE_HABITAT_TABLE = ("create table "
          + TBL_HABITAT + " ("
          + COLUMN_PRIMARY_KEY + " INTEGER PRIMARY KEY AUTO_INCREMENT,"
          + COLUMN_LASTUPDATED + " text,"
          + COLUMN_WORLD + " text,"
          + COLUMN_ID + " integer,"
          + COLUMN_NAME + " text,"
          + COLUMN_MAPX + " integer,"
          + COLUMN_MAPY + " integer,"
          + COLUMN_POINTS + " integer,"
          + COLUMN_CREATIONDATE + " text,"
          + COLUMN_PLAYERID + " integer,"
          + COLUMN_PUBLICTYPE + " text," #use integer 0 2 to store this boolean value later
          + COLUMN_ALLIANCEID + " integer"
          + ")")


    def __init__(self):
    
        # db1 = MySQLdb.connect(host = host, user = user, passwd = passwd)
        # cursor = db1.cursor()
        # sql_createdb = 'CREATE DATABASE ' + dbname
        # cursor.execute(sql_createdb)


        self._host = host,
        self._user = user,
        self._passwd = passwd,
        self._name = dbname,
        self._charset='utf8mb4'
        status = self.connect_db() ##else it gives error about Latin1 cannot encode character. ordinal not in range

        self._cursor.execute('SET NAMES utf8mb4')
        self._cursor.execute("SET CHARACTER SET utf8mb4")
        self._cursor.execute("SET character_set_connection=utf8mb4")


        self._TBL_ALLIANCE = TBL_ALLIANCE
        self._TBL_PLAYER = TBL_PLAYER
        self._TBL_HABITAT = TBL_HABITAT

        self._TBL_ALLIANCE_RAW = TBL_ALLIANCE_RAW
        self._TBL_PLAYER_RAW = TBL_PLAYER_RAW
        self._TBL_HABITAT_RAW = TBL_HABITAT_RAW
        self._TBL_PLAYER_ALL = TBL_PLAYER_ALL
        self._TBL_HABITAT_US3 = TBL_HABITAT_US3

        self._TBL_ACTIVITY_TRACKER = TBL_ACTIVITY_TRACKER
        self._TBL_US3_ACTIVITY_TRACKER_CHANGE = TBL_US3_ACTIVITY_TRACKER_CHANGE


        # self.create(TBL_ALLIANCE, SQL_CREATE_ALLIANCE_TABLE)
        # self.create(TBL_PLAYER, SQL_CREATE_PLAYER_TABLE)
        # self.create(TBL_HABITAT, SQL_CREATE_HABITAT_TABLE)

        #self.convert_columns_to_utf8()

    def connect_db(self):
        try:
            self._db = MySQLdb.connect(host = host, user = user, 
                                 passwd = passwd, port = port, db = dbname, charset = 'utf8mb4')

            self._cursor = self._db.cursor()        
            self._cursor.execute("SELECT VERSION()")
            results = self._cursor.fetchone()
            # Check if anything at all is returned
            if results:
                return True
            else:
                return False
        except MySQLdb.Error, e:
            try:
                print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
            except IndexError:
                print "MySQL Error: %s" % str(e)
                # Print results in comma delimited format
                print "ERROR IN CONNECTION" + str(MySQLdb.Error)
        return False
        

    def convert_columns_to_utf8(self):
        ##to convert the table columns to utf-8, since they can still be different
        self._cursor.execute("ALTER DATABASE `%s` CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci'" % dbname)
        
        sql = "SELECT DISTINCT(table_name) FROM information_schema.columns WHERE table_schema = '%s'" % dbname
        self._cursor.execute(sql)
        
        results = self._cursor.fetchall()
        for row in results:
          sql = "ALTER TABLE `%s` convert to character set DEFAULT COLLATE DEFAULT" % (row[0])
          self._cursor.execute(sql)


    def create(self, table, query):
        sql_tb_exists = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + table + "'"
        ##replace above query statement with the one below that allows for wildcard and format correction
        #sql_tb_exists("""
         # SELECT COUNT(*)
          #FROM information_schema.tables
          #WHERE table_name = '{0}'
          #""".format(tablename.replace('\'', '\'\'')))
        c = self._db.cursor()
        c.execute(sql_tb_exists)
        if c.fetchone()[0] != 1:
          c.execute(query)
          self._db.commit()


    def get_connection(self):
        return self._db

    def execute(self, query, params):
        c = self._db.cursor()
        status = c.execute(query, params)
        self._db.commit()
        return status

    def executemany(self, query, params):
        c = self._db.cursor()
        status = c.executemany(query, params)
        self._db.commit()
        return status


    def cursor(self):
        self._db.cursor()


    def commit(self):
        self._db.commit()


    def close(self):
      if self._db is not None:
        self._db.close()


        
    def main():
        main


if __name__ == "__main__":
    main()

    