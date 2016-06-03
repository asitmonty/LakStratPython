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

COLUMNS_HABITATS = [COLUMN_LASTUPDATED, COLUMN_WORLD, COLUMN_ID, COLUMN_NAME, COLUMN_MAPX, COLUMN_MAPY, COLUMN_POINTS, COLUMN_PLAYERID, COLUMN_PUBLICTYPE, COLUMN_ALLIANCEID]
                            
   
class TblHabitat:

    def __init__(self):
      self._db = databasehelper_mysql.DbHelper()
      self._tblname = self._db._TBL_HABITAT

    def sql_do(self, sql, *params):
      self._db.execute(sql, params)
      self._db.commit()

    ''' # Loads data from the 'tbl_habitat' table into a dataframe
        # Inputs : 
                    1. 'include' :- 1 to use alliances passed as argument. 0 to use all alliances excluding the list of alliances passed as argument
                    2. 'list_players' :- list of players to include or exclude 
        # Output:
                    1. dataframe with selected rows from habitat table
    '''
    def read_from_sql_to_dataframe(self, include, list_players):
      
      select_column_names = [COLUMN_LASTUPDATED, COLUMN_WORLD, COLUMN_ID, COLUMN_NAME, COLUMN_MAPX, COLUMN_MAPY, COLUMN_POINTS, COLUMN_PLAYERID, COLUMN_PUBLICTYPE, COLUMN_ALLIANCEID]
      group_column_names = [COLUMN_PLAYERID, COLUMN_LASTUPDATED, COLUMN_PLAYERPOINTS]
      
      # create the joined strings for column names for use in sql query string
      column_names_string = ",".join(str(x) for x in select_column_names)  # for columns to be extracted, join as string for use in sql query
      group_column_names_string = ",".join(str(x) for x in group_column_names)  # for columns to be grouped by, join as string for use in sql query
      
      # create the sql query string 
      if include == 1:
        sql = ("SELECT " + column_names_string + " FROM " + self._tblname 
            + " WHERE " + COLUMN_PLAYERID + " IN ('" + "','".join(map(str, list_players))
            + "')" 
            #+ " GROUP BY " + group_column_names_string
            )
      else:
            sql = ("SELECT " + column_names_string + " FROM " + self._tblname 
            + " WHERE " + COLUMN_PLAYERID + " NOT IN ('" + "','".join(map(str, list_players))
            + "')" 
            #+ " GROUP BY " + group_column_names_string
            )

      df_player_castle_data = pandas.read_sql(sql, self._db.get_connection())
      #df = self.convert_to_unicode_dtype(df)
      return df_player_castle_data
      
    def read_from_sql_to_dataframe_alliance(self, include, list_alliances):
      
      select_column_names = [COLUMN_LASTUPDATED, COLUMN_WORLD, COLUMN_ID, COLUMN_NAME, COLUMN_MAPX, COLUMN_MAPY, COLUMN_POINTS, COLUMN_PLAYERID, COLUMN_PUBLICTYPE, COLUMN_ALLIANCEID]
      group_column_names = [COLUMN_PLAYERID, COLUMN_LASTUPDATED, COLUMN_PLAYERPOINTS]
      
      # create the joined strings for column names for use in sql query string
      column_names_string = ",".join(str(x) for x in select_column_names)  # for columns to be extracted, join as string for use in sql query
      group_column_names_string = ",".join(str(x) for x in group_column_names)  # for columns to be grouped by, join as string for use in sql query
      
      # create the sql query string 
      if include == 1:
        sql = ("SELECT " + column_names_string + " FROM " + self._tblname 
            + " WHERE " + COLUMN_ALLIANCEID + " IN ('" + "','".join(map(str, list_alliances))
            + "')" 
            #+ " GROUP BY " + group_column_names_string
            )
      else:
            sql = ("SELECT " + column_names_string + " FROM " + self._tblname 
            + " WHERE " + COLUMN_ALLIANCEID + " NOT IN ('" + "','".join(map(str, list_alliances))
            + "')" 
            #+ " GROUP BY " + group_column_names_string
            )

      df_player_castle_data = pandas.read_sql(sql, self._db.get_connection())
      #df = self.convert_to_unicode_dtype(df)
      return df_player_castle_data
      
      
    def insert(self, row):
      format_strings = "(" + ','.join(COLUMNS_HABITATS) + ") values (" + ','.join(['%s'] * len(COLUMNS_HABITATS)) + ")"
      result = self._db.execute("insert into tbl_Habitat %s" % format_strings,
                row)

    
    def write_to_sql(self, pdata):

        pdata.to_sql(self._tblname, self._db.get_connection(), flavor = 'mysql', if_exists='append', index = True, index_label = [COLUMN_ALLIANCERANK, COLUMN_ALLIANCEID, 
        COLUMN_ALLIANCENAME, COLUMN_PLAYERID, COLUMN_NICK, COLUMN_LASTUPDATED])  #cannot use 'replace' since the create table portion fails
                            #with error identifier too long (becasuse of the many foreign keys that violate identifier size <63 rule for msyql)
    
                
    def writeToTable(self, habitat_data):

        dict_obj = dict.fromkeys(COLUMNS_HABITATS)
        for data, value in habitat_data.iteritems():
            #playerIDs are arrays inside the JSON blocks (multiple player IDs inside {})
            #code below runs through the array, and converts it into string to store in SQL column
            
            ####THE DICT_OBJ CREATION CODE HELPS TO ERROR CHECK AGAINST MISSING COLUMN VALUES, WHICH WILL MAKE THE PROGRAM FAIL DURING INSERT
            dict_obj[data] = value  #for all other fields, just copy to the new variable that hosts playerIDs as strings
        
        habitat_values = tuple(dict_obj[property] for property in COLUMNS_HABITATS)  #create an ordered list according to that of columnsHabitats
        self.insert(habitat_values)
        return habitat_data

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
