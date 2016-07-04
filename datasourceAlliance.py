  #database class
import databasehelper_mysql as dbhelper
import pandas
import utilities
import time
import re

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
COLUMN_ALLIANCEID = "id"
COLUMN_ALLIANCERANK = "rank"
COLUMN_ALLIANCENAME = "name"
COLUMN_PLAYERIDs = "playerIDs"
COLUMN_DESC = "description"
COLUMN_NICK = "nick"

WORLD = "US-3"

COLUMNS_ALLIANCE = [COLUMN_WORLD, COLUMN_LASTUPDATED, COLUMN_LASTUPDATE_LNK, COLUMN_ALLIANCEID, COLUMN_DESC, COLUMN_PLAYERIDs, 
                    COLUMN_POINTS, COLUMN_ALLIANCENAME, COLUMN_ALLIANCERANK]

class TblAlliance:

    global _tblname

    def __init__(self):
      self._db = dbhelper.DbHelper()
      self._tblname = dbhelper.TBL_ALLIANCE_RAW

    def open_conn(self):
      self._db = dbhelper.DbHelper()

    def sql_do(self, sql, *params):
      self._db.execute(sql, params)
      self._db.commit()

    def read_distinct(self, column_name):
      sql = "SELECT DISTINCT " + column_name + " FROM " + self._tblname
      c = self._db.execute(sql, None)
      data = c.fetchall()
      return data

    def convert_to_unicode_dtype(self, df):
        types = df.apply(lambda x: pandas.lib.infer_dtype(x.values))
        #types[types=='unicode']
        for col in types[types=='unicode'].index:
            df[col] = df[col].astype(str)
        return df
            
            
    ''' # Loads data from the 'alliance_raw' table into a dataframe
        # Inputs : 
                    1. 
                    2. 
        # Output:
                    1. dataframe 
    '''
    def read_from_sql_to_dataframe(self, include, listalliances):
      start_time = time.time()
      end_time = time.time()
      #ulog.logit(3, "time to write one row: " + utilities.show_elapsed(start_time, end_time))
      select_column_names = [COLUMN_WORLD, COLUMN_LASTUPDATED, COLUMN_LASTUPDATE_LNK, COLUMN_ALLIANCEID, COLUMN_ALLIANCENAME, COLUMN_ALLIANCERANK]
      group_column_names = [COLUMN_WORLD, COLUMN_LASTUPDATED, COLUMN_LASTUPDATE_LNK, COLUMN_ALLIANCEID, COLUMN_ALLIANCENAME, COLUMN_ALLIANCERANK]

      # create the joined strings for column names for use in sql query string
      column_names_string = ",".join(str(x) for x in select_column_names)  # for columns to be extracted, join as string for use in sql query
      group_column_names_string = ",".join(str(x) for x in group_column_names)  # for columns to be grouped by, join as string for use in sql query

      MAX_DATE = "max_date"
      # create the sql query string 
      if include == 1:
        sql = ("SELECT " + column_names_string + " FROM " + self._tblname + " a, "
          + " ( SELECT " + COLUMN_WORLD + " world2, MAX(" + COLUMN_LASTUPDATED + ") "+ MAX_DATE
            + " FROM " + self._tblname 
            + " GROUP BY " + COLUMN_WORLD+ ") b "
            + " WHERE " + COLUMN_ALLIANCEID + " IN ('" + "','".join(map(str, listalliances))
            + "') AND " + COLUMN_LASTUPDATED + " = " + MAX_DATE
            #+ ") AND " + COLUMN_WORLD + " = '" + WORLD + "'"
            + " GROUP BY " + group_column_names_string
            + " ORDER BY " + COLUMN_ALLIANCERANK + " ASC "
            )
      else:
        sql = ("SELECT " + column_names_string + " FROM " + self._tblname + " a, "
          + " ( SELECT " + COLUMN_WORLD + " world2, MAX(" + COLUMN_LASTUPDATED + ") "+ MAX_DATE
            + " FROM " + self._tblname 
            + " GROUP BY " + COLUMN_WORLD+ ") b "
            + " WHERE " + COLUMN_ALLIANCEID + " NOT IN ('" + "','".join(map(str, listalliances))
            + "') AND " + COLUMN_LASTUPDATED + " = " + MAX_DATE
            #+ ") AND " + COLUMN_WORLD + " = '" + WORLD + "'"
            + " GROUP BY " + group_column_names_string
            + " ORDER BY " + COLUMN_ALLIANCERANK + " ASC "
            )

      df_all_alliances = pandas.read_sql(sql, self._db.get_connection())
      #df = self.convert_to_unicode_dtype(df)
      return df_all_alliances




    def insert(self, row):
      format_strings = "(" + ','.join(COLUMNS_ALLIANCE) + ") values (" + ','.join(['%s'] * len(COLUMNS_ALLIANCE)) + ")"
      result = self._db.execute("insert into " + self._tblname + " %s" % format_strings,
              row)


    def writeToTable(self, alliance_data):
      dict_obj = dict.fromkeys(COLUMNS_ALLIANCE)
      #for l in alliance_properties.iteritems
      for name, value in alliance_data.iteritems():
          #playerIDs are arrays inside the JSON blocks (multiple player IDs inside {})
          #code below runs through the array, and converts it into string to store in SQL column
          if name == COLUMN_PLAYERIDs:
              lstToStr =','.join(map(str, value))
              dict_obj[name] = lstToStr  #replace the array with the new string in a new dict variable
          else:
              dict_obj[name] = value  #for all other fields, just copy to the new variable that hosts playerIDs as strings
      alliance_values = tuple(dict_obj[property] for property in COLUMNS_ALLIANCE)  #create an ordered list according to that of columnsAlliance
      self.insert(alliance_values)
      return alliance_data


    def insert_multiple_to_table(self, alliance_data_list):
      list_alliance_tuple = []
      #for l in alliance_properties.iteritems
      for alliance_data in alliance_data_list:
        dict_obj = dict.fromkeys(COLUMNS_ALLIANCE)
        for name, value in alliance_data.iteritems():
            #playerIDs are arrays inside the JSON blocks (multiple player IDs inside {})
            #code below runs through the array, and converts it into string to store in SQL column
            if value == 'None':
              value = 'null'
            dict_obj[name] = value
            if name == COLUMN_PLAYERIDs:
                lstToStr =','.join(map(str, value))
                dict_obj[name] = lstToStr  #replace the array with the new string in a new dict variable
        alliance_tuple = tuple(dict_obj[item] for item in COLUMNS_ALLIANCE)  #create an ordered list according to that of columnsAlliance
        list_alliance_tuple.append(alliance_tuple)
      format_strings = " (" + ','.join(COLUMNS_ALLIANCE) + ") "
      insert_query = "insert into " + self._tblname + format_strings + " VALUES (" + ','.join(['%s'] * len(COLUMNS_ALLIANCE)) + ")"
      result = self._db.executemany(insert_query, list_alliance_tuple)
      return result


    def close(self):
      if self._db is not None:
        self._db.close()



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
