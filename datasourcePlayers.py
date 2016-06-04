  #database class
import databasehelper_mysql as dbhelper

COLUMNS_PLAYERS = ['lastUpdated', 'world', 'id', 'nick', 'habitatIDs', 'allianceID', 'alliancePermission', 'points', 'rank', 'underAttackProtection', 'onVacation']
    
class TblPlayer:

    global _tblname
    def __init__(self):
      self._db = dbhelper.DbHelper()
      self._tblname = dbhelper.TBL_PLAYER_RAW

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
