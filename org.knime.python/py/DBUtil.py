import jaydebeapi
import re
import numpy as np
from pandas import DataFrame
from _ast import TryExcept

def quote_column(columnname):
    match = re.search(r"^[\w\d]+$", columnname)
    if not match:
        return '"' + columnname + '"'
    else:
        return columnname

class DBUtil:

    def __init__(self, sql):
        driver_args = [sql.JDBCUrl, sql.userName, sql.password]
        jclassname = sql.driver
        jars = sql.jars
        self.conn = jaydebeapi.connect(jclassname, driver_args, jars)
        self.conn.jconn.setAutoCommit(False)
        self.cursor = self.conn.cursor()
        self.inputQuery = sql.query
        #stores the name of the result table if any
        self.outputWriter = None
        self.outputQuery = None

    def table_exists(self, tablename):
        query = """Select 1 AS TEST from %s;""" % tablename
        try:
            self.cursor.execute(query)
            return True;
        except Exception as e:
            #print type(e)
            #print e.args      # arguments stored in .args
            #print e
            return False;

    def get_output_query(self):
        if self.outputQuery != None:
            return self.outputQuery
        if self.outputWriter != None:
            return "SELECT * FROM " + self.outputWriter.tablename
        return self.inputQuery
    
    def set_output_query(self, outputQuery):
        self.outputQuery = outputQuery

    def get_cursor(self):
        return self.cursor

    def close_cursor(self):
        self.cursor.close()

    def get_dataframe(self, query=None):
        if query == None:
            self.cursor.execute(self.inputQuery)
        else:
            self.cursor.execute(query)
        
        str_columns = [] # array to store all columns from type string
        columns = [] # array to store all columns names

        for desc in self.cursor.description:
            columns.append(desc[0])
            if 'VARCHAR' in desc[1].values:
                str_columns.append(desc[0])

        df = DataFrame(self.cursor.fetchall(), columns=columns)
        df[str_columns] = df[str_columns].astype(np.character)
        return df

    def write_dataframe(self, tablename, dataframe):
        colDef = dataframe.Column
        get_db_writer(tablename, colDef)
        self.outputWriter.write_many(dataframe.values)
        #self.tablename = tablename
        #self._drop_table(tablename)
        #self._create_table(tablename, dataframe)
        #self._insert_into_table(tablename, dataframe)
        self.conn.commit() 

    def close_connection(self):
        self.conn.close()
