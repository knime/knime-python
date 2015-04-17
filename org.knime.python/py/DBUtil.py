import jaydebeapi
import re
import numpy as np
from pandas import DataFrame
from collections import OrderedDict
import inspect
import tempfile
from datetime import datetime

""" Dictionary contains the mapping of python data types to SQL data types. """
python_db_mapping = {int : 'integer',
                     long : 'integer', 
                     str : 'varchar(255)', 
                     float : 'numeric(30,10)',
                     bool: 'boolean',
                     datetime: 'timestamp'}

class DBUtil(object):
    """An utility class to interact with various databases. """
    def __init__(self, sql):
        """Initializes DBUtil object.
        
        Args:
            sql: A SQL object containing informations to connect to the database.
        """
        self._conn = jaydebeapi.connect(sql.driver, 
                                        [sql.JDBCUrl, sql.userName, sql.password],
                                        sql.jars)
        self._conn.jconn.setAutoCommit(False)
        self._cursor = self._conn.cursor()
        self._output_query = None
        self._input_query = sql.query
        
        if sql.dbIdentifier == 'hive2':
            self._writer = HiveWriter(self)
        else:
            self._writer = GenericWriter(self)
        self._debug = False
        self._quote_character = sql.identifierQuote
        
    def set_quote_character(self, quote_character):
        """Set the quote character for table and column names
            Args:
                quote_character: The quote character to use.
        """
        self._quote_character = quote_character

    def _quote_identifier(self, identifier):
        """Quotes identifier if necessary."""
        if (identifier is None) or (identifier.isspace()):
            raise DBUtilError("Enter a valid identifier.")
        else:
            match = re.search(r"^[\w\d]+$", identifier)
            if not match:
                return self._quote_character + identifier + self._quote_character
            else:
                return str(identifier)
        
    def _set_debug(self, debug):
        """Setting debug to true results in printing of all database queries"""
        self._debug = debug
    
    def _execute_query_savepoint(self, query):
        """Execute a query, if it fails, return to the savepoint before the execution."""
        savepoint = None
        try:
            savepoint = self._conn.set_savepoint()
        except:
            savepoint = None
        try:
            self._execute_query(query)
            if savepoint != None:
                self._conn.release_savepoint(savepoint)
            return True
        except:
            if savepoint != None:
                self._conn.rollback(savepoint)
            return False

    def _execute_query(self, sql, values=None):
        if self._debug:
                print sql
        if values:
            self._cursor.execute(sql, values)
        else:
            self._cursor.execute(sql)
    
    def _executemany(self, sql, values):
        if self._debug:
            print sql
        self._cursor.executemany(sql, values)
    
    def get_output_query(self):
        """Gets output query. """
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
        """Gets hive output."""
        if isinstance(self._writer, HiveWriter):
            return self._writer._get_hive_output()
        else:
            return None
        
    def get_cursor(self):
        """Gets the cursor object."""
        return self._cursor
 
    def close_cursor(self):
        """Closes the cursor object."""
        self.get_cursor().close()

    def _close_connection(self):
        """ Closes the connection. """
        self._conn.close()
        
    def _cleanup(self):
        """Cleans up."""
        self._close_connection()
        
    def get_db_reader(self, query=None):
        """Gets a new DBReader object. For details about DBReader, see the DBReader description below.
        
        Args:
            query: A sql query to retrieve contents from database. Default is 
                None, which means that the input query from DBUtil object is used.
        
        Returns:
            DBReader: A new DBReader object.
        """
        if query == None:
            return DBReader(self.get_cursor(), self._input_query)
        return DBReader(self.get_cursor(), query)
        
    def get_db_writer(self, tablename, col_specs, drop=False, 
                      delimiter="\t", partition_columns=[]):
        """Gets a new DBWriter object.
        
        Args:
            tablename: Name of a new table or an existing one.
            col_specs: A 'DataFrame' object or a 'dict' object containing the 
                column specifications. When a 'DataFrame' object is given, its 
                column specifications will be automatically retrieved and used. 
                Alternatively, a 'dict' object can be given with column names as
                keys and column types as values of the 'dict' object. Column types
                can be either string, which represents SQL data type, or 
                python data types which will be automatically converted to SQL 
                data types.
            drop: If it is true, the existing table will be dropped. Otherwise,
                the data will be appended to the table. Default value is False.
            
        Returns:
            self._writer: A DBWriter object.
        
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
        """Writes dataframe into database.
        
        Args:
            tablename: Name of the table where the dataframe should be written to.
                It can be the name of a new table or an existing one.
            dataframe: A dataframe object to be written into the database.
            drop: If it is true, the existing table will be dropped. Otherwise,
                the dataframe will be appended to the table. Default value is False.
            delimiter: Delimiter for hive table. Default is "\t". 
            partition_columns: Partition columns for hive table.
        """
        self._writer = self.get_db_writer(tablename=tablename, 
                                          col_specs=dataframe, 
                                          drop=drop, 
                                          delimiter=delimiter, 
                                          partition_columns=partition_columns)
        self._writer.write_many(dataframe)
        
    def get_dataframe(self, query=None):
        """Returns the dataframe representation of the input SQL query.
        
        Args:
            query: A SQL query used to build the dataframe. Default is None, which
                means that the input query of DBUtil object is used.
        
        Returns:
            df: A dataframe representation of the input SQL query.
        """
       
        db_reader = self.get_db_reader(query)
        
        str_columns = [] # array to store all columns from type string
        columns = [] # array to store all columns names

        for desc in self.get_cursor().description:
            columns.append(desc[0])
            if desc[1] == jaydebeapi.STRING:
                str_columns.append(desc[0]) 

        df = DataFrame(db_reader.fetchall(), columns=columns)
        df[str_columns] = df[str_columns].astype(np.character)
        return df
    
    def print_description(self):
        """Prints descriptions of this object."""
        self._print_description(self, "DBUtil")
        self._print_description(DBReader, "DBReader")
        self._print_description(DBWriter, "DBWriter")
                    
    def _print_description(self, obj, title):
        """Prints descriptions of this object."""
        
        print title
        print len(title) * "-"
        print
        
        filter_private = lambda x : not(x.startswith('_'))

        # All public methods
        methods = inspect.getmembers(obj, predicate=inspect.ismethod)
        public_methods = filter(filter_private, [method[0] for method in methods])
        
        for m in public_methods:
            print('  ' + str(m) + "():")
            doc = inspect.getdoc(getattr(obj, m))
            if not doc:
                doc = "No description"
            print '    ' , '    '.join(doc.splitlines(True))          
            print        

class DBWriter(object):
    """A class to write data into database."""
    def __init__(self, db_util):
        """Initialize the writer."""
        self._db_util = db_util
        self._tablename = None
        self._col_specs = None
        
    def _has_output_query(self):
        """Check if this writer has output query."""
        return self._tablename != None
    
    def _get_output_query(self):
        """Returns the output query."""
        if self._tablename:
            return "SELECT * FROM " + self._db_util._quote_identifier(self._tablename)
        
        return None
    
    def _set_tablename(self, tablename):
        """Sets table name."""
        if (tablename is None) or (str(tablename).isspace()):
            raise DBUtilError("Enter a valid table name.")
        else:
            self._tablename = tablename
    
    def commit(self):
        """Commits all changes to the database."""
        conn = self._db_util._conn
        if conn:
            conn.commit()
            
    def _fetch_db_metadata(self):
        """Fetch the meta data of a table in the database."""
        query = "SELECT * FROM (SELECT * FROM " + self._db_util._quote_identifier(self._tablename) + ") temp WHERE (1 = 0)"
        success = self._db_util._execute_query_savepoint(query)
        if success:
            return [desc[0] for desc in self._db_util.get_cursor().description]
        else:
            return []
        
    def _table_exists(self):
        """Checks if table exists in the database."""
        query = """SELECT 1 AS tmp FROM %s""" % self._db_util._quote_identifier(self._tablename)
        return self._db_util._execute_query_savepoint(query)
    
    def _drop_table(self):
        """Drops a table in the database."""
        query = """DROP TABLE %s""" % self._db_util._quote_identifier(self._tablename)
        return self._db_util._execute_query_savepoint(query)
        
    def _create_table(self):
        """Creates a new table in the database."""
        try:
            col_list = [(self._db_util._quote_identifier(cname), ctype) for cname, ctype in self._col_specs.iteritems()]

            columns = (',\n').join('%s %s' % col for col in col_list)
            query = """CREATE TABLE %(tablename)s (%(columns)s)"""

            query = query % {'tablename' : self._db_util._quote_identifier(self._tablename), 'columns' : columns}
            self._db_util._execute_query(query)

        except AttributeError as ex:
            raise DBUtilError("'col_specs' must be a 'dict' object with " +
                        "'column names' as keys and 'column types' as values.")
        
    def _build_insert_query(self, columns):
        """Build insert query with column names and wildcards."""
        wildcards = (',').join('?' * len(columns))
        columns = map(self._db_util._quote_identifier, columns)
        cols = (',').join(columns)
        query = """INSERT INTO %s (%s) VALUES (%s)""" %(self._db_util._quote_identifier(self._tablename),
                    cols, wildcards)
        return query
    
    def _get_type_mapping(self, col_specs):
        """Gets type mapping of SQL data type."""
        if isinstance(col_specs, DataFrame):
            return self._get_type_mapping_from_dataframe(col_specs)
        elif isinstance(col_specs, dict):
            return self._get_type_mapping_from_dict(col_specs)
        else:
            raise DBUtilError("'col_specs' must be either a DataFrame object or " +
                              "a 'dict' object with 'column names'" +
                              " as keys and 'column types' as values.")
    
    def _get_type_mapping_from_dict(self, col_specs):
        """Gets type mapping of SQL data type from a 'dict' object."""
        # Set all column names to lowercase and quote them if necessary
        for col_name in col_specs.keys():
            col_specs[str(col_name)] = col_specs.pop(col_name)
            
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
        """Helper method to get SQL data type from dict."""
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
        """Gets type mapping of SQL data type from a 'DataFrame' object."""
        db_metadata = self._fetch_db_metadata()
        dataframe.rename(columns=lambda x: str(x), inplace=True)
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
        """Helper method to get SQL data type from dataframe."""
        if np.issubclass_(col_type, np.floating):
            return "numeric(30,10)"
        elif np.issubclass_(col_type, np.integer):
            return "integer"
        else:
            return "varchar(255)"
        
    def _verify_row(self, input):
        """Verify that all columns in 'input' exist in 'col_specs'"""
        result = OrderedDict()
        cols_not_in_spec = []
        for col in input:
            col_name = str(col)
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
        """Writes a new row into the database.
        
        Args:
            row: A 'dict' object with column names as keys and column values as 
                values of the 'dict' object. Column types can be either string, 
                which represents SQL data type, or python data types which will 
                be automatically converted to SQL data types.
        """
        pass
    
    def write_many(self, dataframe):
        """Writes dataframe into the database.
        
        Args:
            dataframe: A 'DataFrame' object to be written into the database. 
                The column specifications of the 'DataFrame' object must match 
                the 'col_specs' of the DBWriter.
        """
        pass
    
    def _initialize(self, tablename, col_specs, drop):
        pass
    
class GenericWriter(DBWriter):
    """A class to write data into various databases."""
    def __init__(self, db_util):
        super(GenericWriter, self).__init__(db_util)
    
    def _initialize(self, tablename, col_specs, drop):
        """Do some initialization. 
            
        If a table exists and drop is True, the existing table will be dropped
        and a new table will be created.
        """
        self._set_tablename(tablename)
        if self._table_exists() and drop:
            self._drop_table()
       
        self._col_specs = self._get_type_mapping(col_specs)[0]
        
        if not self._table_exists():
            self._create_table()
            
    def write_row(self, row):
        """Writes a new row into the database.
        
        Args:
            row: A 'dict' object with column names as keys and column values as 
                values of the 'dict' object. Column types can be either string, 
                which represents SQL data type, or python data types which will 
                be automatically converted to SQL data types.
        """
        if isinstance(row, dict):
            verified_row = self._verify_row(row)
            query = self._build_insert_query(verified_row.keys())
            self._db_util._execute_query(query, verified_row.values())
        else:
            raise DBUtilError("'row' must be a 'dict' object with 'column names'"
                        " as keys and 'column values' as values of the 'dict' object.")
    
    def write_many(self, dataframe):
        """Writes dataframe into the database.
        
        Args:
            dataframe: A 'DataFrame' object to be written into the database. 
                The column specifications of the 'DataFrame' object must match 
                the 'col_specs' of the DBWriter.
        """
        if isinstance(dataframe, DataFrame):
            verified_row = self._verify_row(list(dataframe))
            query = self._build_insert_query(verified_row.keys())
            self._db_util._executemany(query, dataframe.values)
        else:
            raise DBUtilError("The input parameter must be a 'DataFrame' object.")
        
    
    
class HiveWriter(DBWriter):
    """A class to write data into a hive table."""
    def __init__(self, db_util):
        super(HiveWriter, self).__init__(db_util)
        self._hive_output = None
    
    def _get_hive_output(self):
        return self._hive_output
            
    def _initialize(self, tablename, col_specs, drop):
        """Do some initialization."""
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
        """Sets delimiter and partition columns for the hive table."""
        self._hive_output['delimiter'] = delimiter
        self._hive_output['partitionColumnNames'] = partition_columns
    
    def _set_col_specs(self, col_specs):
        """Sets 'col_specs' of the DBWriter."""
        specs, db_cols_not_in_col_specs = self._get_type_mapping(col_specs)
        if db_cols_not_in_col_specs: # some db columns are not in col_specs
            raise DBUtilError("Hive Error: Some columns in database doesn't " +
                              "exist in 'col_specs'. Not existing columns: " +
                              str(db_cols_not_in_col_specs))
        else:
            self._col_specs = specs
    
    def write_row(self, row):
        """Writes a new row into the database.
        
        Args:
            row: A 'dict' object with column names as keys and column values as 
                values of the 'dict' object. Column types can be either string, 
                which represents SQL data type, or python data types which will 
                be automatically converted to SQL data types.
        """
        if isinstance(row, dict):
            if len(row) == len(self._col_specs):
                self._verify_row(row)
                ordered_row = [row.get(col) for col in self._col_specs.keys()]
                row_text = (self._hive_output.get('delimiter')).join(ordered_row)
                if self._file.closed:
                    self._file = open(self._file.name, 'a')
                self._file.write(row_text + "\n")
            else:
                raise DBUtilError("Hive Error: The input row has " + len(row) + 
                                  " columns, but the 'col_specs' has " + 
                                  len(self._col_specs) + " columns. The columns " +
                                  "of the input row must correspond to the columns of " +
                                  "the 'col_spec'.")
        else:
            raise DBUtilError("'row' must be a 'dict' object with 'column names'"
                        " as keys and 'column values' as values of the 'dict' object.")
    
    def write_many(self, dataframe):
        """Writes dataframe into the database.
        
        Args:
            dataframe: A 'DataFrame' object to be written into the hive table. 
                The column specifications of the 'DataFrame' object must match 
                the 'col_specs' of the DBWriter.
        """
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
        self._close_file()
            
    def _close_file(self):
        if self._file:
            self._file.close()

class DBReader(object):
    """A class to read data from the database."""
    def __init__(self, cursor, query):
        self._cursor = cursor
        self._cursor.execute(query)

    def fetchone(self):
        """ Fetch the next row of a query result set.
        
        Returns:
            result: A single sequence containing the data of the fetched row or
                None when no more data is available.
        """
        result = self._cursor.fetchone()
        return result
    
    def fetchmany(self, size=None):
        """Fetch the next set of rows of a query result set.
        
        Args:
            size: The number of rows to fetch per call. If it is not given, the 
                cursor's arraysize determines this number. This method tries to 
                fetch as many rows as indicated by the size parameter. If this 
                is not possible due to the specified number of rows not being 
                available, fewer rows may be returned. 
        
        Returns:
            result: A set of rows as a sequence of sequences (e.g. a list of tuples).
                An empty sequence is returned when no more rows are available. 
        """
        if not size:
            size = self._cursor.arraysize
        result = self._cursor.fetchmany(size)
        return result

    def fetchall(self):
        """Fetch all (remaining) rows of a query result set.
        
        Returns:
            result: all (remaining) rows as a sequence of sequences (e.g. a list
                of tuples).
        """
        result = self._cursor.fetchall()
        return result

class DBUtilError(Exception):
    def __init__(self, message):
        self.message = message
    
    def __str__(self):
        return self.message
        