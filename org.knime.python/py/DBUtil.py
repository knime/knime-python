import jaydebeapi
import re
import numpy as np
from pandas import DataFrame
from collections import OrderedDict
import inspect

python_db_mapping = {int : 'integer',
                     long : 'integer', 
                     str : 'varchar(255)', 
                     float : 'numeric(30,10)',
                     bool: 'boolean'}

def _quote_column(columnname):
    if (columnname is None) or (columnname.isspace()):
        exit("ERROR: Enter a valid column name")
    else:
        match = re.search(r"^[\w\d]+$", columnname)
        if not match:
            return '"' + columnname + '"'
        else:
            return str(columnname)
    
def _get_db_col_specs(col_specs):
    if isinstance(col_specs, DataFrame):
        return _get_db_col_specs_from_dataframe(col_specs);
    else:
        if isinstance(col_specs, dict):
            specs = OrderedDict()
            for name, ctype in col_specs.iteritems():
                if not(isinstance(ctype, basestring) or isinstance(ctype, type)):
                    exit("ERROR: column type '" + str(ctype) + 
                         "' must be either string or python data type")
                elif isinstance(ctype, basestring):
                    specs[_quote_column(name)] = ctype
                elif isinstance(ctype, type):
                    db_type = python_db_mapping.get(ctype)
                    if db_type == None:
                        specs[_quote_column(name)] = "varchar(255)"
                    else:
                        specs[_quote_column(name)] = db_type
            
            return specs
        else:
            exit("ERROR: 'col_specs' must be either a DataFrame object or " +
                    "a 'dict' object with 'column names'" +
                 " as keys and 'column types' as values")

def _get_db_col_specs_from_dataframe(dataframe):
    specs = OrderedDict()
    for name in dataframe.columns:
        if np.issubclass_(dataframe[name].dtype.type, np.floating):
            sqltype = 'numeric(30,10)'
        elif np.issubclass_(dataframe[name].dtype.type, np.integer):
            sqltype = 'integer'
        else:
            sqltype = 'varchar(255)'
        specs[_quote_column(name)] = sqltype

    return specs

class DBUtil(object):
    """ A class used to interact with databases.
    
    Longer description here.
    
    Attributes:
        conn: description.
        cursor: description.
        input_query: description.
        output_query: description.
        output_writer: description.
    """
    def __init__(self, sql):
        """Initializes DBUtil object.
        
        Args:
            sql: A SQL object containing informations to connect to database.
        """
        self.conn = jaydebeapi.connect(sql.driver, 
                                       [sql.JDBCUrl, sql.userName, sql.password], 
                                       sql.jars)
        self.conn.jconn.setAutoCommit(False)
        self.cursor = self.conn.cursor()
        self.input_query = sql.query
        #stores the name of the result table if any
        self.output_writer = None
        self.output_query = None
        self._dummy = None
    
    def get_output_query(self):
        """Gets output query.
        
        Longer description here.
        
        Returns:
            output_query:
        
        """
        if self.output_query != None:
            return self.output_query
        if self.output_writer != None:
            return "SELECT * FROM " + self.output_writer.tablename
        return self.input_query
    
    def set_output_query(self, outputQuery):
        """Sets output query."""
        self.output_query = outputQuery

    def get_db_reader(self, query=None):
        """Get a new instance of DBReader.
        
        Args:
            query: A sql query to retrieve contents from database. Default is 
                None, which means that the input_query from DBUtil object is used.
        
        Returns:
            DBReader: A new instance of DBReader.
        """
        if query == None:
            return DBReader(self.conn, self.input_query)
        return DBReader(self.conn, query)

    def get_db_writer(self, tablename, col_specs):
        """Gets a new instance of DBWriter
        
        Args:
            tablename: The name of the table where the data should be written to.
            col_specs: A DataFrame object or a dict object containing the 
                columns specifications.
            
        Returns:
            output_writer: A new instance of DBWriter.
        
        """
        self.output_writer = DBWriter(self.conn, tablename, col_specs)
        return self.output_writer

    def get_cursor(self):
        """Gets the cursor object."""
        return self.cursor

    def close_cursor(self):
        """Closes the cursor object."""
        self.cursor.close()

    def get_dataframe(self, query=None):
        """ get dataframe """
        if query == None:
            self.cursor.execute(self.input_query)
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
        """ write dataframe """
        self.output_writer = self.get_db_writer(tablename, dataframe)
        self.output_writer.write_many(dataframe)
        #self.output_writer.insert_into_table(dataframe)
        #self.tablename = tablename
        #self._drop_table(tablename)
        #self._create_table(tablename, dataframe)
        #self._insert_into_table(tablename, dataframe)
        self.output_writer.commit() 

    def close_connection(self):
        """ close connection """
        self.conn.close()
        
    def print_description(self):
        """ Prints descriptions of this object. """
        # All public instance attributes
        filter_private = lambda x : not(x.startswith('_'))
        attrs = filter(filter_private, self.__dict__.keys())
        print 'ATTRIBUTES ...'
        for at in attrs:
            print at
        
        print
        # All public methods
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        results = filter(filter_private, [m[0] for m in methods])
        print 'METHODS ...'
        for r in results:
            print r, ":"
            print inspect.getdoc(getattr(self, r))
            print
        
        


class DBReader(object):

    def __init__(self, conn, query):
        self.cursor = conn.cursor()
        self.cursor.execute(query)

    def fetchone(self):
        result = self.cursor.fetchone()
        return result
    
    def fetchmany(self, size):
        result = self.cursor.fetchmany(size)
        return result

    def fetchall(self):
        result = self.cursor.fetchall()
        return result

    def close_cursor(self):
        self.cursor.close()

    def get_cursor(self):
        return self.cursor


class DBWriter(object):

    def __init__(self, conn, tablename, col_specs):
        self.conn = conn
        self.cursor = conn.cursor()
        self.col_specs = _get_db_col_specs(col_specs)
        if (tablename is None) or (str(tablename).isspace()):
            exit("ERROR: Enter a valid table name")
        else:
            self.tablename = tablename 
        #self._create_table()
        '''
        if drop:
            self._drop_table()
            self._create_table(notexists=False)
        else:
            self._create_table(notexists=True)
        '''

    def commit(self):
        self.conn.commit()

    def get_cursor(self):
        return self.cursor

    def close_cursor(self):
        self.cursor.close()
        
    def get_query(self):
        wildcards = (',').join('?' * len(self.col_specs))
        col_names = (',').join(col for col in self.col_specs.keys())
        query = """INSERT INTO %s (%s) VALUES (%s)""" %(self.tablename,
                    col_names, wildcards)
        return query

    def write_row(self, row):
        if isinstance(row, dict):
            query = self.get_query()
            print query
            print row.values()
            values = []
            total_none = 0
            for name in self.col_specs.keys():
                value = row.get(name)
                values.append(value)
                if value is None:
                    total_none += 1
            
            if total_none == len(row):
                exit("ERROR: Nothing is inserted. All columns in 'row' " + 
                    "do not exist in table '" + str(self.tablename) + "'.")
            print values
            self.cursor.execute(query, values)
        else:
            exit("ERROR: 'row' must be a 'dict' object with 'column names'"
                  " as keys and 'column values' as values")
                  
    def write_many(self, dataframe):
        query = self.get_query()
        self.cursor.executemany(query, dataframe.values)     
            
    def _drop_table(self):
        query = """DROP TABLE %s""" % self.tablename
        try:
            self.cursor.execute(query)
            return True
        except Exception as ex:
            print type(ex)
            print ex.args
            print "ERROR DROP"
            return False
        
    def _table_exists(self):
        query = """SELECT 1 AS test FROM %s""" % self.tablename
        savepoint = None
        try:
            savepoint = self.conn.set_savepoint()
        except Exception as ex:
            savepoint = None
        try:
            self.cursor.execute(query)
            if savepoint != None:
                self.conn.release_savepoint(savepoint)
            return True
        except Exception as ex:
            print type(ex)
            print ex.args
            if savepoint != None:
                self.conn.rollback(savepoint)
            #self.conn.commit()
            return False
            
    def _create_table(self):
        try:
            col_list = []
            for col_name, col_type in self.col_specs.iteritems():
               col_name = _quote_column(col_name)
               col_list.append((col_name, col_type))

            columns = (',\n').join('%s %s' % col for col in col_list)
            query = """CREATE TABLE %(tablename)s (
                            %(columns)s
                        );"""
            query = query % {'tablename' : self.tablename, 'columns' : columns}
            self.cursor.execute(query)

        except AttributeError:
            exit("ERROR: 'col_specs' must be a 'dict' object with 'column names'"
                  " as keys and 'column types' as values")

   
    #===================================================================================================================
    # def _create_table(self, tablename, dataframe):
    #     column_types = []
    #     for idx, name in enumerate(dataframe.columns):
    #         if np.issubclass_(dataframe[name].dtype.type, np.floating):
    #             sqltype = 'numeric(30,10)'
    #         elif np.issubclass_(dataframe[name].dtype.type, np.integer):
    #             sqltype = 'integer'
    #         else:
    #             sqltype = 'varchar(255)'
    #         '''
    #         elif np.issubclass_(type(dataframe[name][0]), str):
    #             sqltype = 'varchar(255)'
    #         else:
    #             sqltype = 'blob'
    #         '''
    #         name = quote_column(name)
    #         column_types.append((name, sqltype))
    #         
    #     columns = (',\n').join('%s %s' % col for col in column_types)
    #     query = """CREATE TABLE %(tablename)s (
    #                     %(columns)s
    #                 );"""
    #     query = query % {'tablename' : tablename, 'columns' : columns}
    #     self.cursor.execute(query)
    #===================================================================================================================