  #database class
import databasehelper_mysql
import pandas

COLUMN_LASTUPDATED = "lastUpdated"
COLUMN_WORLD = "world"
COLUMN_ID = "id"
COLUMN_NAME = "name"
COLUMN_MAPX = "mapX"
COLUMN_MAPY = "mapY"
COLUMN_POINTS = "points"
COLUMN_PLAYERID = "playerID"
COLUMN_PUBLICTYPE = "publicType"
COLUMN_ALLIANCEID = "allianceId"
COLUMN_ALLIANCERANK = "allianceRank"
COLUMN_ALLIANCENAME = "allianceName"
COLUMN_NICK = "nick"
COLUMN_PLAYERPOINTS = "playerPoints"

COLUMNS_ACTIVITY_TRACKER = [COLUMN_LASTUPDATED, COLUMN_PLAYERID, COLUMN_NICK, COLUMN_PLAYERPOINTS, COLUMN_ALLIANCEID, 
                            COLUMN_ALLIANCENAME, COLUMN_ALLIANCERANK]
    
class TblActivityTracker:

    def __init__(self):
      self._db = databasehelper_mysql.DbHelper()
      self._tblname = self._db._TBL_US3_ACTIVITY_TRACKER_CHANGE
      
    def sql_do(self, sql, *params):
      self._db.execute(sql, params)
      self._db.commit()

    def read_distinct(self, column_name):
      sql = "SELECT DISTINCT " + column_name + " FROM " + self._tblname
      c = self._db.execute(sql, None)
      data = c.fetchall()
      return data

      
    def delete(self):
      sql = "DELETE FROM " + self._tblname
      c = self._db.execute(sql, None)
      
      
    def convert_to_unicode_dtype(self, df):
        types = df.apply(lambda x: pandas.lib.infer_dtype(x.values))
        #types[types=='unicode']
        for col in types[types=='unicode'].index:
            df[col] = df[col].astype(str)
        return df
            
            
    ''' # Loads data from the 'player_activity_tracker' table into a dataframe
        # Inputs : 
                    1. 'include' :- 1 to use alliances passed as argument. 0 to use all alliances excluding the list of alliances passed as argument
                    2. 'listalliances' :- list of alliances to include or exclude 
        # Output:
                    1. dataframe with selected rows from activity tracker table
    '''
    def load_from_dataframe(self, include, df_activity_table):
      
      select_column_names = [COLUMN_LASTUPDATED, COLUMN_ALLIANCEID, COLUMN_ALLIANCENAME, COLUMN_ALLIANCERANK, COLUMN_PLAYERID, COLUMN_NICK, COLUMN_PLAYERPOINTS]
      group_column_names = [COLUMN_PLAYERID, COLUMN_LASTUPDATED, COLUMN_PLAYERPOINTS]
      
      # create the joined strings for column names for use in sql query string
      column_names_string = ",".join(str(x) for x in select_column_names)  # for columns to be extracted, join as string for use in sql query
      group_column_names_string = ",".join(str(x) for x in group_column_names)  # for columns to be grouped by, join as string for use in sql query
      
      # create the sql query string 
      if include == 1:
        sql = ("SELECT " + column_names_string + " FROM " + self._tblname 
            + " WHERE " + COLUMN_ALLIANCEID + " IN ('" + "','".join(map(str, listalliances))
            + "') GROUP BY " + group_column_names_string)
      else:
            sql = ("SELECT " + column_names_string + " FROM " + self._tblname 
            + " WHERE " + COLUMN_ALLIANCEID + " NOT IN ('" + "','".join(map(str, listalliances))
            + "') GROUP BY " + group_column_names_string)
      #cur = self._db.execute(sql, None)

      df_player_activity = pandas.read_sql(sql, self._db.get_connection())
      #df = self.convert_to_unicode_dtype(df)
      return df_player_activity

    def insert(self, row):
      format_strings = "(" + ','.join(COLUMNS_HABITATS) + ") values (" + ','.join(['%s'] * len(COLUMNS_HABITATS)) + ")"
      result = self._db.execute("insert into tbl_Habitat %s" % format_strings,
                row)

    def write_to_sql(self, pdata):

        pdata.to_sql(self._tblname, self._db.get_connection(), flavor = 'mysql', if_exists='append', index = False)  #cannot use 'replace' since the create table portion fails
                            #with error identifier too long (becasuse of the many foreign keys that violate identifier size <63 rule for msyql)
                
               
    def writeToTable(self, habitat_jdata):
        dict_obj = dict.fromkeys(COLUMNS_HABITATS)
        for data, value in habitat_jdata.iteritems():
            #playerIDs are arrays inside the JSON blocks (multiple player IDs inside {})
            #code below runs through the array, and converts it into string to store in SQL column
            
            ####THE DICT_OBJ CREATION CODE HELPS TO ERROR CHECK AGAINST MISSING COLUMN VALUES, WHICH WILL MAKE THE PROGRAM FAIL DURING INSERT
            dict_obj[data] = value  #for all other fields, just copy to the new variable that hosts playerIDs as strings
        
        habitat_values = tuple(dict_obj[property] for property in COLUMNS_HABITATS)  #create an ordered list according to that of columnsHabitats
        self.insert(habitat_values)
        return habitat_jdata

    @property
    def filename(self):
      return self._filename

    @filename.setter
    def filename(self, fn):
      self._filename = fn
      self._db = sqlite3.connect(fn)
      self._db.row_factory = sqlite3.Row

    @property
    def table(self): return self._table

    @table.setter
    def table(self, t): self._table = t


    def main():
      main


if __name__ == "__main__":
    main()
