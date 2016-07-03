  #database class
import databasehelper_mysql as dbhelper
import re
import pandas


COLUMN_LASTUPDATE_LNK = "lastUpdateLnk"
COLUMN_WORLD = "world"
COLUMN_PLAYERID = "id"
COLUMN_NICK = "nick"
COLUMN_HABITATIDS = "habitatIDs"
COLUMN_ALLIANCEID = "allianceID"
COLUMN_ALLIANCEPERM = "alliancePermission"
COLUMN_POINTS = "points"
COLUMN_RANK = "rank"
COLUMN_UNDERATTACKPROT = "underAttackProtection"
COLUMN_ONVACATION = "onVacation"

COLUMNS_PLAYERS = [COLUMN_LASTUPDATE_LNK, COLUMN_WORLD, COLUMN_PLAYERID, COLUMN_ALLIANCEID]
COLUMNS_PLAYERS_ADD = [COLUMN_LASTUPDATE_LNK, COLUMN_WORLD, COLUMN_PLAYERID, COLUMN_NICK, COLUMN_HABITATIDS, 
                COLUMN_ALLIANCEID, COLUMN_POINTS, COLUMN_RANK, COLUMN_ALLIANCEPERM, COLUMN_UNDERATTACKPROT, COLUMN_ONVACATION]

class TblPlayer:

    global _tblname
    def __init__(self):
      self._db = dbhelper.DbHelper()
      self._tblname = dbhelper.TBL_PLAYER

    def open_conn(self):
      self._db = dbhelper.DbHelper()


    def close(self):
      if self._db is not None:
        self._db.close()



    def sql_do(self, sql, *params):
      self._db.execute(sql, params)
      self._db.commit()

    def insert(self, row):
      format_strings = "(" + ','.join(COLUMNS_PLAYERS) + ") values (" + ','.join(['%s'] * len(COLUMNS_PLAYERS)) + ")"
      result = self._db.execute("insert into  " + self._tblname + " %s" % format_strings,
                row)

    def writeToTable(self, player_data):

        dict_obj = dict.fromkeys(COLUMNS_PLAYERS)
        #for l in alliance_properties.iteritems
        for data, value in player_data.iteritems():
            #playerIDs are arrays inside the JSON blocks (multiple player IDs inside {})
            #code below runs through the array, and converts it into string to store in SQL column
            if data == 'habitatIDs':
                lstToStr =','.join(map(str, value))
                dict_obj[data] = lstToStr  #replace the array with the new string in a new dict variable
            else:
                dict_obj[data] = value  #for all other fields, just copy to the new variable that hosts playerIDs as strings
        player_values = tuple(dict_obj[property] for property in COLUMNS_PLAYERS)  #create an ordered list according to that of columnsAlliance
        self.insert(player_values)
        return player_data


    def insert_multiple_to_table(self, player_data_list):
      list_player_tuple = []
      #for l in alliance_properties.iteritems
      for player_data in player_data_list:
        dict_obj = dict.fromkeys(COLUMNS_PLAYERS_ADD)
        for name, value in player_data.iteritems():
            #playerIDs are arrays inside the JSON blocks (multiple player IDs inside {})
            #code below runs through the array, and converts it into string to store in SQL column
            if value == 'None':
              value = 'null'
            dict_obj[name] = value
            if name == 'habitatIDs':
                lstToStr =','.join(map(str, value))
                dict_obj[name] = lstToStr  #replace the array with the new string in a new dict variable
        player_tuple = tuple(dict_obj[item] for item in COLUMNS_PLAYERS_ADD)  #create an ordered list according to that of columnsAlliance
        list_player_tuple.append(player_tuple)
      format_strings = " (" + ','.join(COLUMNS_PLAYERS_ADD) + ") "
      insert_query = "insert into " + self._tblname + format_strings + " VALUES (" + ','.join(['%s'] * len(COLUMNS_PLAYERS_ADD)) + ")"
      result = self._db.executemany(insert_query, list_player_tuple)
      return result


    def read_from_sql_to_dataframe_world(self, include, world):
      max_date = "max_date"
      select_column_names = [COLUMN_PLAYERID, COLUMN_ALLIANCEID]
      # create the joined strings for column names for use in sql query string
      column_names_string = ",".join(str(x) for x in select_column_names)  # for columns to be extracted, join as string for use in sql query
      # create the sql query string 
      
      if include == 1:
        sql = ("SELECT " + column_names_string + " FROM " + self._tblname + " , " 
            + "(SELECT MAX(" + COLUMN_LASTUPDATE_LNK + ") as " + max_date 
            + " FROM " + self._tblname 
            + " WHERE " + COLUMN_WORLD + " = '" + world + "') temp"
            + " WHERE " + COLUMN_WORLD + " = '" + world + "' "
            + " AND " + COLUMN_LASTUPDATE_LNK + " = temp." + max_date 
            + " GROUP BY " + column_names_string
            )
      df_player_castle_data = pandas.read_sql(sql, self._db.get_connection())
      #df = self.convert_to_unicode_dtype(df)
      return df_player_castle_data



    def insert(self, row):
      format_strings = "(" + ','.join(COLUMNS_HABITATS) + ") values (" + ','.join(['%s'] * len(COLUMNS_HABITATS)) + ")"
      result = self._db.execute("insert into " + self._tblname + " %s" % format_strings,
                row)



    @property
    def filename(self):
      return self._filename

    @filename.setter
    def filename(self, fn):
      self._filename = fn
      self._db = sqlite3.connect(fn)
      self._db.row_factory = sqlite3.Row

    @property
    def table(self): return self._tblname

    @table.setter
    def settable(self, t): self._tblname = t


    def main():
      main


if __name__ == "__main__":
    main()
