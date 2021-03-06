  #database class
import MySQLdb

import sys

class DbHelper:

    global SQLITE_DB
    global TBL_ALLIANCE
    global TBL_PLAYER
    global TBL_HABITAT
    global SQL_CREATE_ALLIANCE_TABLE
    global SQL_CREATE_PLAYER_TABLE
    global SQL_CREATE_HABITAT_TABLE
    global host
    global passwd
    global user
    global dbname

    host = "localhost"
    passwd = "nellierova"
    user = "root"
    #dbname = "db_lnk_v9 - us3 all feb 2 night"
    dbname = "db_lnk_02"
    
    TBL_ALLIANCE = "tbl_alliance"
    TBL_PLAYER = "tbl_player"
    TBL_HABITAT = "tbl_habitat"

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
    COLUMN_ALLIANCEID = "allianceId"
    COLUMN_ALLIANCEPERM = "alliancePermission"
    COLUMN_UNDERATTACKPROT = "underAttackProtection"
    COLUMN_ONVACATION = "onVacation"

    COLUMN_MAPX = "mapX"
    COLUMN_MAPY = "mapY"
    COLUMN_CREATIONDATE = "creationDate"
    COLUMN_PLAYERID = "playerID"
    COLUMN_PUBLICTYPE = "publicType"
    COLUMN_WORLD = "world"
    COLUMN_LASTUPDATED = "lastUpdated"
    
    
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

      self._db = MySQLdb.connect(host = host,
        user = user,
        passwd = passwd,
        db = dbname,
        charset='utf8') ##else it gives error about Latin1 cannot encode character. ordinal not in range
      self._cursor = self._db.cursor()

      self._cursor.execute('SET NAMES utf8mb4')
      self._cursor.execute("SET CHARACTER SET utf8mb4")
      self._cursor.execute("SET character_set_connection=utf8mb4")

      DbHelper._TBLALLIANCE = TBL_ALLIANCE
      DbHelper._TBLPLAYER = TBL_PLAYER
      DbHelper._TBLHABITAT = TBL_HABITAT

      self.create(TBL_ALLIANCE, SQL_CREATE_ALLIANCE_TABLE)
      self.create(TBL_PLAYER, SQL_CREATE_PLAYER_TABLE)
      self.create(TBL_HABITAT, SQL_CREATE_HABITAT_TABLE)

      #self.convert_columns_to_utf8()

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

    def execute(self, query, params):

      c = self._db.cursor()

      c.execute(query, params)
      self._db.commit()

    def cursor(self):
      self._db.cursor()


    def commit(self):
      self._db.commit()

      
    def main():
      main


if __name__ == "__main__":
    main()
