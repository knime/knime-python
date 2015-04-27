import jaydebeapi
import re
import numpy as np
from pandas import DataFrame
import pandas as pd
from collections import OrderedDict
import inspect
import tempfile
import codecs
from datetime import datetime
from __builtin__ import int

""" Dictionary contains the mapping of python data types to SQL data types."""
python_db_mapping = {int : 'integer',
                     long : 'numeric(30,10)', 
                     str : 'varchar(255)', 
                     float : 'numeric(30,10)',
                     bool: 'boolean',
                     datetime: 'timestamp'}

""" Dictionary contains the mapping of python data types to Hive data types."""
python_hive_mapping = {int : 'int',
                     long : 'bigint', 
                     str : 'string', 
                     float : 'double',
                     bool: 'boolean',
                     datetime: 'timestamp'}

class DBUtil(object):
    """An utility class to interact with various databases."""
    def __init__(self, sql):
        """Initializes DBUtil object.
        
        Args:
            sql: A SQL object containing informations to connect to the database.
        """
        self._conn = jaydebeapi.connect(sql.driver, 
                                        [sql.JDBCUrl, sql.userName, sql.password],
                                        sql.jars)
            
        self._conn.jconn.setAutoCommit(False)
        #sql_key_words_str = meta_data.getSQLKeywords()
        #self.sql_key_words = set(sql_key_words_str.split(",")) 
        self._cursor = self._conn.cursor()
        self._output_query = None
        self._input_query = sql.query
        self._db_identifier = sql.dbIdentifier
        self._type_mapping = python_db_mapping
        
        self._quote_character = None
        try:
            meta_data = self._conn.getMetaData()
            self._quote_character = meta_data.getIdentifierQuoteString()
        except:
            pass
        if not self._quote_character or len(self._quote_character.strip()) < 1:
            if self._db_identifier == 'hive2':
                self._quote_character = "`"
            else:
                self._quote_character = "\""
        
        if self._db_identifier == 'hive2':
            self._writer = HiveWriter(self)
        else:
            self._writer = GenericWriter(self)
        self._quote_all_identifier = True
        self._debug = False
    
    def _quote_identifier(self, identifier):
        """Quotes identifier if necessary."""
        if (identifier is None) or (len(str(identifier).strip()) < 1):
            raise DBUtilError("Enter a valid identifier. Use set_quote_character() of DBUtil.")
        else:
            if self._quote_all_identifier or not re.search(r"^[\w\d]+$", identifier):
                return self._quote_character + identifier + self._quote_character
            else:
                return str(identifier)
        
    def _set_debug(self, debug):
        """Setting debug to true results in printing of all database queries."""
        self._debug = debug
        
    def _get_hive_output(self):
        """Gets hive output."""
        if isinstance(self._writer, HiveWriter):
            return self._writer._get_hive_output()
        else:
            return None
        
    def _close_connection(self):
        """ Closes the connection."""
        self._conn.close()
        
    def _cleanup(self):
        """Cleans up."""
        self.close_cursor()
        self._close_connection()
        
    def _table_exists(self, tablename):
        """Checks if table exists in the database."""
        query = """SELECT 1 AS tmp FROM %s""" % self._quote_identifier(tablename)
        return self._execute_query_savepoint(query)
    
    def _drop_table(self, tablename):
        """Drops a table in the database."""
        query = """DROP TABLE %s""" % self._quote_identifier(tablename)
        return self._execute_query_savepoint(query)
        
    def _create_table(self, tablename, col_specs):
        """Creates a new table in the database."""    
        col_specs = self._get_type_mapping(col_specs)
        
        columns = (',\n').join('%s %s' % (self._quote_identifier(cname), ctype) 
                               for cname, ctype in col_specs.iteritems())
        
        query = """CREATE TABLE %(tablename)s (%(columns)s)"""

        query = query % {'tablename' : self._quote_identifier(tablename), 
                         'columns' : columns}
        self._execute_query(query)
        return col_specs
        
    def _build_insert_query(self, tablename, columns):
        """Build insert query with column names and wildcards."""
        wildcards = (',').join('?' * len(columns))
        columns = map(self._quote_identifier, columns)
        cols = (',').join(columns)
        query = """INSERT INTO %s (%s) VALUES (%s)""" %(self._quote_identifier(tablename),
                cols, wildcards)
        
        return query
    
    def _fetch_db_metadata(self, tablename):
        """Fetch the meta data of a table in the database."""
        query = "SELECT * FROM (SELECT * FROM " + self._quote_identifier(tablename) + ") temp WHERE (1 = 0)"
        success = self._execute_query_savepoint(query)
        if success:
            db_metadata = [desc[0] for desc in self.get_cursor().description]
            if isinstance(self._writer, HiveWriter):
                # Trim 'tablename' from hive column names
                for idx, col in enumerate(db_metadata):
                    db_metadata[idx] = self._fix_hive_col_name(col)
            return db_metadata;
        else:
            return []
    
    def _execute_query_savepoint(self, query):
        """Execute a query, if it fails, return to the savepoint before the execution."""
        savepoint = None
        try:
            savepoint = self._conn.set_savepoint()
        except:
            if self._debug:
                print "Save points not supported by db"
            savepoint = None
        query_result = False
        try:
            self._execute_query(query)
            query_result = True
            if savepoint != None:
                try:
                    self._conn.release_savepoint(savepoint)
                except Exception as e:
                    if self._debug:
                        print "Exception when releasing save point"
        except Exception as e:
            if self._debug:
                print "Save point query failed"
            if savepoint != None:
                try:
                    self._conn.rollback(savepoint)
                except Exception as e:
                    if self._debug:
                        print "Save point rollback failed"
        return query_result
    
    def _use_tez_for_hive(self):
        '''Sets TEZ as execution engine for Hive which increases the performance. 
        However not all Hive installations support tez.'''
        self._execute_query("set hive.execution.engine=tez");
    
    def _execute_query(self, sql, values=None):
        """Execute a SQL query."""
        if self._debug:
                print sql
        if values is not None:
            self._cursor.execute(sql, values)
        else:
            self._cursor.execute(sql)
    
    def _executemany(self, sql, values):
        """Execute query with list of values."""
        if (self._db_identifier == "oracle"):
            for value in values:
                self._execute_query(sql, value)
            return
        if self._debug:
            print sql
        self._cursor.executemany(sql, values)
        
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
        specs = OrderedDict()
        for col_name, col_type in col_specs.iteritems():
            try:
                specs[col_name] = self._get_db_type(col_type)
            except DBUtilError as ex:
                raise DBUtilError(str(ex) + " Column '" + str(col_name) + 
                                  "' has type '" + str(col_type) + "'.")
                
        return specs
    
    def _get_db_type(self, col_type):
        """Helper method to get SQL data type from dict.""" 
        if isinstance(col_type, basestring):
            return col_type
        elif isinstance(col_type, type):
            db_type = self._type_mapping[col_type]
            if not db_type:
                return self._type_mapping[str] # default type if no mapping exists
            return db_type
        else:
            raise DBUtilError("column type must be either string or " +
                                  "python data type.")
    
    def _get_type_mapping_from_dataframe(self, dataframe):
        """Gets type mapping of SQL data type from a 'DataFrame' object."""
        specs = OrderedDict()
        for col_name in dataframe:
            specs[col_name] = self._get_db_type_from_dataframe(
                                            dataframe[col_name].dtype.type)
        return specs
    
    def _get_db_type_from_dataframe(self, col_type):
        """Helper method to get SQL data type from DataFrame."""
        if np.issubclass_(col_type, np.floating):
            return self._get_db_type(float)
        elif np.issubclass_(col_type, np.integer):
            return self._get_db_type(int)
        elif np.issubclass_(col_type, np.datetime64):
            return self._get_db_type(datetime)
        else:
            return self._get_db_type(str)
        
    def _print_description(self, obj, title, all):
        """Prints descriptions of this object."""        
        print title
        print len(title) * "-"
        print
        
        filter_private = lambda x : all or not(x.startswith('_'))

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
    
    def set_quote_all_identifier(self, quote_all):
        """By default all identifiers are quoted. If set to false only identifier with a space are quoted.
            
        Args:
            quote_all: False if only identifier with a space should be quoted.
        """
        self._quote_all_identifier = quote_all
    
    def set_quote_character(self, quote_character):
        """Set the quote character for table and column names.
        
        Args:
            quote_character: The quote character to use.
        """
        self._quote_character = quote_character
        
    def set_type_mapping(self, type_mapping):
        """Dictionary for python type to SQL type lookups. 
            Default mapping: 
                {int : 'integer',
                long : 'integer', 
                str : 'varchar(255)', 
                float : 'numeric(30,10)',
                bool: 'boolean',
                datetime: 'timestamp'}
            
        Args:
            type_mapping: Type dictionary to map python type to SQL type.
        """
        self._type_mapping = type_mapping
        
    def get_output_query(self):
        """Gets the output query that is passed to KNIME and used at the nodes outport. """
        if self._output_query != None:
            return self._output_query
        elif self._writer._has_output_query():
            return self._writer._get_output_query()
        else:
            return self._input_query
    
    def set_output_query(self, output_query):
        """Overwrites the output query that is passed to KNIME and used at the nodes outport."""
        self._output_query = output_query
               
    def get_cursor(self):
        """Gets the cursor object (https://www.python.org/dev/peps/pep-0249/#cursor-objects)."""
        return self._cursor
 
    def close_cursor(self):
        """Closes the cursor object."""
        self.get_cursor().close()
        
    def get_db_reader(self, query=None):
        """Gets a new DBReader object. The DBReader object is used to retrieve information from the database.
        For details about DBReader, see the DBReader description below.
        
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
        """Gets a new DBWriter object. THe DBWriter is used to write data to the database.
        For details about DBWriter, see the DBWriter description below.
        
        Args:
            tablename: Name of a new table or an existing one.
            col_specs: A 'DataFrame' object or a 'dict' object containing the 
                column specifications. When a 'DataFrame' object is given, its 
                column specifications will be automatically retrieved and used. 
                Alternatively, a 'dict' object can be given with column names as
                keys and column types as values of the 'dict' object. Column types
                can be either string, which represents SQL data type, or 
                python data types which will be automatically converted to SQL 
                data types. To preserve the order of col_specs, use OrderedDict 
                (from collections import OrderedDict) instead of dict.
            drop: If it is true, the existing table will be dropped. Otherwise,
                the data will be appended to the table. Default value is False.
            delimiter: Delimiter for Hive table. Default is "\t".
            partition_columns: Partition columns for Hive table.
            
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
            delimiter: Delimiter for Hive table. Default is "\t". 
            partition_columns: Partition columns for Hive table.
        """
        self._writer = self.get_db_writer(tablename=tablename, 
                                          col_specs=dataframe, 
                                          drop=drop, 
                                          delimiter=delimiter, 
                                          partition_columns=partition_columns)
        self._writer.write_many(dataframe)

    def _fix_hive_col_name(self, col_name):
        """Hive's JDBC drivers always adds the table name to the column names. 
        This method returns the column name without the table name.
        
        Args:
            col_name: The column name as returned by the Hive driver.
        
        Returns:
            cleaned_col_name: The input column name without the table name.
        """
        col_name_array = col_name.split('.')
        cleaned_col_name = col_name_array[len(col_name_array) - 1]
        return cleaned_col_name

    def get_dataframe(self, query=None):
        """Returns the dataframe representation of the input SQL query.
        
        Args:
            query: A SQL query used to build the dataframe. Default is None, which
                means that the input query of DBUtil object is used.
        
        Returns:
            df: A dataframe representation of the input SQL query.
        """
        db_reader = self.get_db_reader(query)
        col_maps = {'all_columns':[], 'datetime_columns':[]}
        #Fetch the meta data prior fetching all values because for SQLite  the meta data are invalidated 
        #after the end of the result set
        for desc in self.get_cursor().description:
            col_name = desc[0]
            if isinstance(self._writer, HiveWriter):
                # Trim 'tablename' from hive column names
                col_name = self._fix_hive_col_name(col_name)
            col_maps.get('all_columns').append(col_name)
            if desc[1] == jaydebeapi.DATETIME:
                col_maps.get('datetime_columns').append(desc[0])
        df = DataFrame(db_reader.fetchall())
        df.columns = col_maps.get('all_columns')
        df[col_maps.get('datetime_columns')] = df[col_maps.get('datetime_columns')].astype('datetime64[ns]')
        
        return df
    
    def print_description(self, all=False):
        """Prints descriptions of this object.
        
        Args:
            all: If it is true, it prints all available methods including
                private methods. Otherwise, it only prints public methods.
        
        """
        self._print_description(self, "DBUtil", all)
        self._print_description(DBReader, "DBReader", all)
        self._print_description(DBWriter, "DBWriter", all)
                    
class DBWriter(object):
    """A class to write data into database."""
    def __init__(self, db_util):
        """Initialize the writer."""
        self._db_util = db_util
        self._tablename = None
        self._col_specs = None
        
    def _initialize(self, tablename, col_specs, drop):
        pass
        
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
        if (tablename is None) or (len(str(tablename).strip()) < 1):
            raise DBUtilError("Enter a valid table name.")
        else:
            self._tablename = tablename
    
    def commit(self):
        """Commits all changes to the database."""
        conn = self._db_util._conn
        if conn:
            conn.commit()
    
    def write_row(self, row):
        """Writes a new row into the database.
        
        Args:
            row: Either a 'dict' object with 'column names' as keys and 'column values' as 
                values of the 'dict' object or a 'list' or 'tuple' containing 'column values'
                that match the table specifications in the database.
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
    
class GenericWriter(DBWriter):
    """A class to write data into various databases."""
    def __init__(self, db_util):
        super(GenericWriter, self).__init__(db_util)
        self._db_util.set_type_mapping(python_db_mapping)
    
    def _initialize(self, tablename, col_specs, drop):
        """Do some initialization. 
            
        If a table exists and drop is True, the existing table will be dropped
        and a new table will be created.
        """
        self._set_tablename(tablename)
        table_exists = self._db_util._table_exists(self._tablename)
        if table_exists and drop:
            self._db_util._drop_table(self._tablename)

        if not table_exists or drop:
            self._col_specs = self._db_util._create_table(self._tablename, col_specs)
        else:
            self._col_specs = self._db_util._get_type_mapping(col_specs)
            
    def write_row(self, row):
        """Writes a new row into the database.
        
        Args:
            row: Either a 'dict' object with 'column names' as keys and 'column values' as 
                values of the 'dict' object or a 'list' or 'tuple' containing 'column values'
                that match the table specifications in the database.
        """
        if isinstance(row, dict):
            query = self._db_util._build_insert_query(self._tablename, row.keys())
            self._db_util._execute_query(query, row.values())
        elif isinstance(row, (list, tuple)):
            query = self._db_util._build_insert_query(self._tablename, self._col_specs.keys())
            self._db_util._execute_query(query, row)
        else:
            raise DBUtilError("'row' must be either a 'dict' object with 'column names'" +
                        " as keys and 'column values' as values of the 'dict' object" +
                        " or a 'list' or 'tuple' containing 'column values' that match" + 
                        " the table specifications in the database.")
    
    def write_many(self, dataframe):
        """Writes dataframe into the database.
        
        Args:
            dataframe: A 'DataFrame' object to be written into the database. 
                The column specifications of the 'DataFrame' object must match 
                the columns specifications of the table in the database.
        """
        if isinstance(dataframe, DataFrame):
            query = self._db_util._build_insert_query(self._tablename, list(dataframe))
            self._db_util._executemany(query, dataframe.values)
        else:
            raise DBUtilError("The input parameter must be a 'DataFrame' object.")
        
    
class HiveWriter(DBWriter):
    """A class to write data into a hive table."""
    def __init__(self, db_util):
        super(HiveWriter, self).__init__(db_util)
        self._db_util.set_type_mapping(python_hive_mapping)
        self._hive_output = None    
            
    def _initialize(self, tablename, col_specs, drop):
        """Do some initialization."""
        self._set_tablename(tablename)
        self._set_col_specs(col_specs, drop)
        self._hive_output = {}
        self._file = tempfile.NamedTemporaryFile(prefix=self._tablename + '_' ,
                                                suffix='.csv', delete=False,
                                                mode='a')
        self._hive_output['tableName'] = self._tablename
        self._hive_output['columnNames'] = self._col_specs.keys()
        self._hive_output['columnType'] = self._col_specs.values()
        self._hive_output['fileName'] = self._file.name
        self._hive_output['tableExist'] = self._db_util._table_exists(self._tablename)
        self._hive_output['dropTable'] = drop
        
    def _set_delimiter_and_partitions(self, delimiter, partition_columns):
        """Sets delimiter and partition columns for the hive table."""
        self._hive_output['delimiter'] = delimiter
        self._hive_output['partitionColumnNames'] = partition_columns
    
    def _set_col_specs(self, col_specs, drop):
        """Sets 'col_specs' of the DBWriter."""
        db_metadata = self._db_util._fetch_db_metadata(self._tablename)
        
        # verify the input col_specs if table exists in database 
        # and drop is False. The col_specs must match the column specifications
        # of the table in the database
        if not drop and db_metadata: 
            self._col_specs = self._verify_col_specs_with_db(col_specs, db_metadata)
        else: # table doesn't exist in database or drop is True
            mapping = self._db_util._get_type_mapping(col_specs)
            for col in mapping.keys():
                mapping[col.lower()] = mapping.pop(col)
            self._col_specs = mapping
        
    def _verify_col_specs_with_db(self, col_specs, db_metadata):
        """Verify that the input col_specs match the column specifications of the
            table in database."""
        if isinstance(col_specs, dict):
            return self._verify_dict_col_specs(col_specs, db_metadata)
        elif isinstance(col_specs, DataFrame):
            return self._verify_dataframe_col_specs(col_specs, db_metadata)
            
    def _verify_dict_col_specs(self, col_specs, db_metadata):
        """Helper method to verify 'dict' input col_specs."""
        # Convert all column names to lowercase
        for col_name in col_specs.keys():
            col_specs[str(col_name).lower()] = col_specs.pop(col_name)
        
        cols_not_in_db = col_specs.keys()
        db_cols_not_in_col_specs = []
        specs = OrderedDict()
        for col_name in db_metadata:
            col_type = col_specs.get(col_name)
            if col_type:
                cols_not_in_db.remove(col_name)
                specs[col_name] = self._db_util._get_db_type(col_type)
            else:
                db_cols_not_in_col_specs.append(col_name)
                    
        if len(cols_not_in_db) > 0:
            raise DBUtilError("Hive Error: Some columns in 'col_specs' " +
                              "do not exist in database; Not existing columns: " +
                              str(cols_not_in_db) + ".")
            
        if len(db_cols_not_in_col_specs) > 0:
            raise DBUtilError("Hive Error: Some columns in database do not " +
                              "exist in 'col_specs'. Not existing columns: " +
                              str(db_cols_not_in_col_specs))
                
        return specs
    
    def _verify_dataframe_col_specs(self, dataframe, db_metadata):
        """Helper method to verify 'DataFrame' input col_specs."""
        dataframe.rename(columns=lambda x: str(x).lower(), inplace=True)
        cols_not_in_db = list(dataframe)
        db_cols_not_in_col_specs = []
        specs = OrderedDict()
        
        for col_name in db_metadata:
            try:
                cols_not_in_db.remove(col_name)
                specs[col_name] = self._db_util._get_db_type_from_dataframe(
                                        dataframe[col_name].dtype.type)
            except ValueError:
                db_cols_not_in_col_specs.append(col_name)
                
        if len(cols_not_in_db) > 0:
            raise DBUtilError("Hive Error: Some columns in 'col_specs' " +
                              "do not exist in database; Not existing columns: " +
                              str(cols_not_in_db) + ".")
            
        if len(db_cols_not_in_col_specs) > 0:
            raise DBUtilError("Hive Error: Some columns in database do not " +
                              "exist in 'col_specs'. Not existing columns: " +
                              str(db_cols_not_in_col_specs))
            
        return specs
            
    def _get_hive_output(self):
        """Gets hive output."""
        return self._hive_output
        
    def _verify_input_row(self, input_row):
        """Verify that all columns in 'input_row' exist in 'col_specs'."""
        result = OrderedDict()
        cols_not_in_spec = []
                
        for col in input_row:
            col_lower = str(col).lower()
            if self._col_specs.has_key(col_lower):
                if isinstance(input_row, dict):
                    result[col_lower] = input_row[col]
                elif isinstance(input_row, (list, tuple)):
                    result[col_lower] = col
            else:
                cols_not_in_spec.append(col)
        
        if len(cols_not_in_spec) > 0:
            raise DBUtilError("Some columns in input_row do not exist in 'col_specs';" +
                              " Not existing columns: " + str(cols_not_in_spec) + ".")
            
        return result
    
    def _close_file(self):
        """Closes file."""
        if self._file and not self._file.closed:
            self._file.close()
            
    def _verify_row_length(self, row):
        """Helper method to verify that 'input_row' has the same length as 'col_specs'."""
        if len(row) != len(self._col_specs):
            raise DBUtilError("Hive Error: The input has " + str(len(row)) + 
                              " columns, but the 'col_specs' has " + 
                              str(len(self._col_specs)) + " columns. The columns " +
                              "of the input must match the columns of " +
                              "the 'col_specs'.")
            
    def _write_row_to_file(self, row, verified_row=None):
        """Helper method to write a row into a text file."""
        delimiter = self._hive_output.get('delimiter')
        row_text = ""
        for val in row:
            if verified_row:
                val = verified_row.get(val)
            
            if isinstance(val, basestring) and "\n" in val:
                raise DBUtilError("Hive Error: Line break characters in cell " + 
                                  "contents are not supported.")
            elif not val:
                val = "\\N"
                    
            val = str(val).replace(delimiter, str("\\" + delimiter))
            row_text += val + delimiter
        self._file.close()
        if self._file.closed:
            self._file  = codecs.open(self._file.name,'a',encoding='utf-8')
        text_2_write = row_text + "\n"
        self._file.write(text_2_write.encode('utf-8'))
    
    def write_row(self, row):
        """Writes a new row into the database.
        
        Args:
            row: Either a 'dict' object with 'column names' as keys and 'column values' as 
                values of the 'dict' object or a 'list' or 'tuple' containing 'column values'
                that match the table specifications in the database.
        """
        if isinstance(row, dict):
            self._verify_row_length(row)
            verified_row = self._verify_input_row(row)
            self._write_row_to_file(self._col_specs.keys(), verified_row)
        elif isinstance(row, (list, tuple)):
            self._verify_row_length(row)
            self._write_row_to_file(row, verified_row=None)
        else:
            raise DBUtilError("'row' must be either a 'dict' object with 'column names'" +
                        " as keys and 'column values' as values of the 'dict' object" +
                        " or a list or tuple containing 'column values' that match" +
                        " the table specifications in the database.")
    
    def write_many(self, dataframe):
        """Writes dataframe into the database.
        
        Args:
            dataframe: A 'DataFrame' object to be written into the hive table. 
                The column specifications of the 'DataFrame' object must match 
                the 'col_specs' of the DBWriter.
        """
        if isinstance(dataframe, DataFrame):
            self._verify_row_length(dataframe.columns)
            result = self._verify_input_row(list(dataframe))
            dataframe.columns = result.keys()
            rows = dataframe.to_dict('records')
            for row in rows:
                self._write_row_to_file(result, row)
        else:
            raise DBUtilError("The input parameter must be a 'DataFrame' object.")
            
    def commit(self):
        """Flush text into file."""
        if self._file and not self._file.closed:
            self._file.flush()
        self._close_file()
            
class DBReader(object):
    """A class to read data from the database."""
    def __init__(self, cursor, query):
        """Initialize DBReader."""
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
    """DBUtil exception."""
    def __init__(self, message):
        self.message = message
    
    def __str__(self):
        return self.message
        