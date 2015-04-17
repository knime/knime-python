import jaydebeapi
import re
import numpy as np
from pandas import DataFrame
from collections import OrderedDict
import inspect
import tempfile
from datetime import datetime

python_db_mapping = {int : 'integer',
                     long : 'integer', 
                     str : 'varchar(255)', 
                     float : 'numeric(30,10)',
                     bool: 'boolean',
                     datetime: 'timestamp'}

def _quote_column(columnname):
    if (columnname is None) or (columnname.isspace()):
        raise DBUtilError("Enter a valid column name.")
    else:
        match = re.search(r"^[\w\d]+$", columnname)
        if not match:
            return '"' + columnname + '"'
        else:
            return str(columnname)

class DBUtil(object):
    """ A class used to interact with databases.
    
    Longer description here.
    columns = self._verify_row(row.keys())
    Attributes:
        _input_query: description.
        _output_query: description.
        _output_writer: description.
    """
    def __init__(self, sql):
        """Initializes DBUtil object.
        
        Args:
            sql: A SQL object containing informations to connect to database.
        """
        self._conn = jaydebeapi.connect(sql.driver, 
                                        [sql.JDBCUrl, sql.userName, sql.password],
                                        sql.jars)
        self._conn.jconn.setAutoCommit(False)
        self._cursor = self._conn.cursor()
        self._output_query = None
        self._input_query = sql.query
        
        if sql.dbIdentifier == 'hive2':
            self._writer = HiveWriter(self._cursor)
        else:
            self._writer = GenericWriter(self._cursor)
    
    def get_output_query(self):
        """Gets output query.
        
        Longer description here.
        
        Returns:
            _output_query:
        
        """
        if self._output_query != None:
            return self._output_query
        elif self._writer._has_output_query():
            return self._writer._get_output_query()
        else:
            return self._input_query
    
    def set_output_query(self, output_query):
        """Sets output query."""
        self._output_query = output_query
        
    def _get_hive_output(self):
        if isinstance(self._writer, HiveWriter):
            return self._writer._get_hive_output()
        else:
            return None
        
    def get_cursor(self):
        """Gets the cursor object."""
        
        return self._cursor
 
    def close_cursor(self):
        """Closes the cursor object."""
        self._cursor.close()

    def _close_connection(self):
        """ close connection """
        self._conn.close()
        
    def _destroy(self):
        self._close_connection()
        
    def get_db_reader(self, query=None):
        """Get a new instance of DBReader.
        
        Args:
            query: A sql query to retrieve contents from database. Default is 
                None, which means that the _input_query from DBUtil object is used.
        
        Returns:
            DBReader: A new instance of DBReader._unknownSqlTypeConverter
        """
        if query == None:
            return DBReader(self._cursor, self._input_query)
        return DBReader(self._cursor, query)
        
    def get_db_writer(self, tablename, col_specs, drop=False, 
                      delimiter="\t", partition_columns=[]):
        """Gets a new instance of DBWriter
        
        Args:
            _tablename: The name of the table where the data should be written to.
            col_specs: A DataFrame object or a dict object containing the 
                columns specifications.
            drop: If it is true, the existing table will be dropped. Otherwise,
                the data will be appended to the table. Default value is False.
            
        Returns:
            _output_writer: A new instance of DBWriter.
        
        """
        self._writer._initialize(tablename=tablename, 
                                col_specs=col_specs, 
                                drop=drop)
        
        if isinstance(self._writer, HiveWriter):
            self._writer._set_delimiter_and_partitions(
                                        delimiter=delimiter, 
                                        partition_columns = partition_columns)

        return self._writer
    
    def write_dataframe(self, tablename, dataframe, drop=False,
                        delimiter="\t", partition_columns=[]):
        self._writer = self.get_db_writer(tablename=tablename, 
                                          col_specs=dataframe, 
                                          drop=drop, 
                                          delimiter=delimiter, 
                                          partition_columns=partition_columns)
        self._writer.write_many(dataframe)
        self._writer.commit()
        
    def get_dataframe(self, query=None):
        """ get dataframe """
       
        db_reader = self.get_db_reader(query)
        
        str_columns = [] # array to store all columns from type string
        columns = [] # array to store all columns names

        for desc in self.get_cursor().description:
            columns.append(desc[0])
            if 'VARCHAR' in desc[1].values:
                str_columns.append(desc[0]) 

        df = DataFrame(db_reader.fetchall(), columns=columns)
        df[str_columns] = df[str_columns].astype(np.character)
        return df
        
    def print_description(self):
        """ Prints descriptions of this object. """
        # All public instance attributes
        filter_private = lambda x : not(x.startswith('_'))
        attrs = filter(filter_private, self.__dict__.keys())

        print 'ATTRIBUTES'
        for at in attrs:
            print '\t', at
        
        print
        # All public methods
        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        public_methods = filter(filter_private, [m[0] for m in methods])
        print 'METHODS'
        for m in public_methods:
            print '\t', m, ":"
            print '\t\t' , '\t\t'.join(inspect.getdoc(getattr(self, m)).splitlines(True))
            print        

class DBWriter(object):

    def __init__(self, cursor):
        self._cursor = cursor
        self._tablename = None
        self._col_specs = None
        
    def _has_output_query(self):
        return self._tablename != None
    
    def _get_output_query(self):
        if self._tablename:
            return "SELECT * FROM " + self._tablename
        
        return None
    
    def _set_tablename(self, tablename):
        if (tablename is None) or (str(tablename).isspace()):
            raise DBUtilError("Enter a valid table name.")
        else:
            self._tablename = tablename
    
    def commit(self):
        if self._conn:
            self._conn.commit()
            
    def _fetch_db_metadata(self):
        query = "SELECT * FROM (SELECT * FROM " + self._tablename + ") temp WHERE (1 = 0)"
        success = self._execute_query_savepoint(query)
        if success:
            return [_quote_column(desc[0].lower()) for desc in self._cursor.description]
        else:
            return []
        
    def _table_exists(self):
        query = """SELECT 1 AS tmp FROM %s""" % self._tablename
        return self._execute_query_savepoint(query)
    
    def _drop_table(self):
        query = """DROP TABLE %s""" % self._tablename
        return self._execute_query_savepoint(query)
    
    def _execute_query_savepoint(self, query):
        savepoint = None
        try:
            savepoint = self._conn.set_savepoint()
        except:
            savepoint = None
        try:
            self._cursor.execute(query)
            if savepoint != None:
                self._conn.release_savepoint(savepoint)
            return True
        except:
            if savepoint != None:
                self._conn.rollback(savepoint)
            return False
        
    def _create_table(self):
        try:
            col_list = [(cname, ctype) for cname, ctype in self._col_specs.iteritems()]

            columns = (',\n').join('%s %s' % col for col in col_list)
            query = """CREATE TABLE %(tablename)s (
                            %(columns)s
                        );"""
            query = query % {'tablename' : self._tablename, 'columns' : columns}
            print query
            self._cursor.execute(query)
            print "HELLO"

        except AttributeError as ex:
            raise DBUtilError("'col_specs' must be a 'dict' object with " +
                        "'column names' as keys and 'column types' as values.")
        
    def _build_insert_query(self, columns):
        wildcards = (',').join('?' * len(columns))
        cols = (',').join(columns)
        query = """INSERT INTO %s (%s) VALUES (%s)""" %(self._tablename,
                    cols, wildcards)
        return query
    
    def _get_type_mapping(self, col_specs):
        if isinstance(col_specs, DataFrame):
            return self._get_type_mapping_from_dataframe(col_specs)
        elif isinstance(col_specs, dict):
            return self._get_type_mapping_from_dict(col_specs)
        else:
            raise DBUtilError("'col_specs' must be either a DataFrame object or " +
                              "a 'dict' object with 'column names'" +
                              " as keys and 'column types' as values.")
    
    def _get_type_mapping_from_dict(self, col_specs):
        # Set all column names to lowercase and quote them if necessary
        for col_name in col_specs.keys():
            col_specs[_quote_column(str(col_name).lower())] = col_specs.pop(col_name)
            
        db_metadata = self._fetch_db_metadata()
        cols_not_in_db = col_specs.keys()
        db_cols_not_in_col_specs = []
        specs = OrderedDict()
        if db_metadata: # table exist in database
            for col_name in db_metadata:
                col_type = col_specs.get(col_name)
                if col_type:
                    cols_not_in_db.remove(col_name)
                    db_type = self._get_db_type(col_type)
                    if db_type:
                        specs[col_name] = db_type
                    else:
                        raise DBUtilError("column type must be either string or " +
                                          "python data type. Column '" + col_name + 
                                          "' has type '" + str(col_type) + "'.")
                else: # col in DB doesn't exist in col_specs
                    db_cols_not_in_col_specs.append(col_name)
                        
            if len(cols_not_in_db) > 0:
                raise DBUtilError("Some columns in 'col_specs' do not exist in " +
                                  "database; Not existing columns: " + 
                                  str(col_not_in_db) + ".")
                    
        else: # table does not exist in database
            for col_name, col_type in col_specs.iteritems():
                db_type = self._get_db_type(col_type)
                if db_type:
                    specs[col_name] = db_type
                else:
                    raise DBUtilError("column type must be either string or " +
                                      "python data type. Column '" + col_name + 
                                      "' has type '" + str(col_type) + "'.")
                        
        return specs, db_cols_not_in_col_specs
    
    def _get_db_type(self, col_type):
        if not(isinstance(col_type, basestring) or isinstance(col_type, type)):
            return None
                        
        if isinstance(col_type, basestring):
            return col_type
        elif isinstance(col_type, type):
            db_type = python_db_mapping(col_type)
            if not db_type:
                return "varchar(255)" # default type if no mapping exists
            return db_type
    
    def _get_type_mapping_from_dataframe(self, dataframe):
        db_metadata = self._fetch_db_metadata()
        dataframe.rename(columns=lambda x: _quote_column(str(x).lower()), inplace=True)
        cols_not_in_db = list(dataframe)
        specs = OrderedDict()
        db_cols_not_in_col_specs = []
        if db_metadata: # table exists in database
            for col_name in db_metadata:
                try:
                    cols_not_in_db.remove(col_name)
                    specs[col_name] = self._get_db_type_from_dataframe(
                                                dataframe[col_name].dtype.type)
                except ValueError:
                    db_cols_not_in_col_specs.append(col_name)
                    
            if len(cols_not_in_db) > 0:
                raise DBUtilError("Some columns in 'col_specs' do not exist in " +
                                  "database; Not existing columns: " + 
                                  str(cols_not_in_db) + ".")
        else: # table does not exist in database
            for col_name in dataframe:
                specs[col_name] = self._get_db_type_from_dataframe(
                                                dataframe[col_name].dtype.type)
        
        return specs, db_cols_not_in_col_specs
        
    def _get_db_type_from_dataframe(self, col_type):
        if np.issubclass_(col_type, np.floating):
            return "numeric(30,10)"
        elif np.issubclass_(col_type, np.integer):
            return "integer"
        else:
            return "varchar(255)"
        
    def _verify_row(self, input):
        """ verify that all columns in 'input' exist in 'col_specs'"""
        result = OrderedDict()
        cols_not_in_spec = []
        for col in input:
            col_name = _quote_column(str(col).lower())
            if self._col_specs.has_key(col_name):
                if isinstance(input, dict):
                    result[col_name] = input[col]
                elif isinstance(input, (list, tuple)):
                    result[col_name] = col
            else:
                cols_not_in_spec.append(col)
        
        if len(cols_not_in_spec) > 0:
            raise DBUtilError("Some columns in input do not exist in 'col_specs';" +
                              " Not existing columns: " + str(cols_not_in_spec) + ".")
            
        return result
    
    def write_row(self, row):
        pass
    
    def write_many(self, dataframe):
        pass
    
    def _initialize(self, tablename, col_specs, drop):
        pass
    
class GenericWriter(DBWriter):
    def __init__(self, sql):
        super(GenericWriter, self).__init__(sql)
    
    def _initialize(self, tablename, col_specs, drop):
        self._set_tablename(tablename)
        if self._table_exists() and drop:
            self._drop_table()
        
        self._col_specs = self._get_type_mapping(col_specs)[0]
        
        if not self._table_exists():
            self._create_table()
            self.commit()
            
    def write_row(self, row):
        if isinstance(row, dict):
            verified_row = self._verify_row(row)
            query = self._build_insert_query(verified_row.keys())
            self._cursor.execute(query, verified_row.values())
        else:
            raise DBUtilError("'row' must be a 'dict' object with 'column names'"
                        " as keys and 'column values' as values.")
    
    def write_many(self, dataframe):
        if isinstance(dataframe, DataFrame):
            verified_row = self._verify_row(list(dataframe))
            query = self._build_insert_query(verified_row.keys())
            self._cursor.executemany(query, dataframe.values)
        else:
            raise DBUtilError("The input parameter must be a 'DataFrame' object.")
        
    
    
class HiveWriter(DBWriter):
    def __init__(self, sql):
        super(HiveWriter, self).__init__(sql)
        self._hive_output = None
    
    def _get_hive_output(self):
        return self._hive_output
            
    def _initialize(self, tablename, col_specs, drop):
        self._set_tablename(tablename)
        self._set_col_specs(col_specs)
        self._hive_output = {}
        self._file = tempfile.NamedTemporaryFile(prefix=self._tablename + '_' ,
                                                suffix='.csv', delete=False,
                                                mode='a')
        self._hive_output['tableName'] = self._tablename
        self._hive_output['columnNames'] = self._col_specs.keys()
        self._hive_output['columnType'] = self._col_specs.values()
        self._hive_output['fileName'] = self._file.name
        self._hive_output['tableExist'] = self._table_exists()
        self._hive_output['dropTable'] = drop
        
    def _set_delimiter_and_partitions(self, delimiter, partition_columns):
        self._hive_output['delimiter'] = delimiter
        self._hive_output['partitionColumnNames'] = partition_columns
    
    def _set_col_specs(self, col_specs):
        specs, db_cols_not_in_col_specs = self._get_type_mapping(col_specs)
        if db_cols_not_in_col_specs: # some db columns are not in col_specs
            raise DBUtilError("Hive Error: Some columns in database doesn't " +
                              "exist in 'col_specs'. Not existing columns: " +
                              str(db_cols_not_in_col_specs))
        else:
            self._col_specs = specs
    
    def write_row(self, row):
        if isinstance(row, dict):
            if len(row) == len(self._col_specs):
                self._verify_row(row)
                ordered_row = [row.get(col) for col in self._col_specs.keys()]
                row_text = (self._hive_output.get('delimiter')).join(ordered_row)
                self._file.write(row_text + "\n")
            else:
                raise DBUtilError("Hive Error: The input row has " + len(row) + 
                                  " columns, but the 'col_specs' has " + 
                                  len(self._col_specs) + " columns. The columns " +
                                  "of the input row must correspond to the columns of " +
                                  "the 'col_spec'.")
        else:
            raise DBUtilError("'row' must be a 'dict' object with 'column names'"
                        " as keys and 'column values' as values.")
    
    def write_many(self, dataframe):
        if isinstance(dataframe, DataFrame):
            if len(dataframe.columns) == len(self._col_specs):
                result = self._verify_row(list(dataframe))
                dataframe.columns = result.keys()
                dataframe.to_csv(self._file.name, index=False, mode='a',
                                 columns=self._col_specs.keys(), header=False,
                                 sep=self._hive_output.get('delimiter'))
            else:
                raise DBUtilError("Hive Error: The input dataframe has " + 
                                  len(dataframe.columns) + " columns, but the " +
                                  "'col_specs' has " + len(self._col_specs) + 
                                  " columns. The columns of the input dataframe " +
                                  "must correspond to the columns of the 'col_spec'.")
        else:
            raise DBUtilError("The input parameter must be a 'DataFrame' object.")
    
    def commit(self):
        if self._file:
            self._file.flush()
            
    def _close_file(self):
        if self._file:
            self._file.close()

class DBReader(object):

    def __init__(self, cursor, query):
        #self._cursor = conn.cursor()
        self._cursor = cursor
        self._cursor.execute(query)

    def fetchone(self):
        result = self._cursor.fetchone()
        return result
    
    def fetchmany(self, size):
        result = self._cursor.fetchmany(size)
        return result

    def fetchall(self):
        result = self._cursor.fetchall()
        return result

class DBUtilError(Exception):
    def __init__(self, message):
        self.message = message
    
    def __str__(self):
        return self.message
        