import os
from datetime import date
import json

from simple_database.exceptions import ValidationError
from simple_database.config import BASE_DB_FILE_PATH


class Row(object):
    def __init__(self, row):
        for key, value in row.items():
            setattr(self, key, value)


class Table(object):

    def __init__(self, db, name, columns=None):
        self.db = db
        self.name = name

        self.table_filepath = os.path.join(BASE_DB_FILE_PATH, self.db.name,
                                           '{}.json'.format(self.name))

        if not os.path.exists(self.table_filepath):
            with open(self.table_filepath, 'a') as fp:
                fp.write(json.dumps({'columns': columns, 'rows': []}))

        self.columns = columns or self._read_columns()

    def _read_columns(self):
        # Read the columns configuration from the table's JSON file
        # and return it.
        with open(self.table_filepath, 'r') as fp:
            return json.load(fp)['columns']

    def insert(self, *args):
        rows_dict = {}
        if len(args) != len(self.columns):
            raise ValidationError('Invalid amount of field')

        for i, col in enumerate(self.columns):
            if col['type'] == type(args[i]).__name__:
                rows_dict[col['name']] = args[i]
            else:
                raise ValidationError('Invalid type of field "{}": Given "{}", expected "{}"'.format(col['name'],type(args[i]).__name__, col['type']))
        
        self._write_to_file(rows_dict)
    
    def _write_to_file(self, row):
        with open(self.table_filepath, 'r+') as fp:
            json_dict = json.load(fp)
            json_dict['rows'].append(row)
            fp.seek(0)
            fp.write(json.dumps(json_dict, default=self._serialize_date))
            
    def query(self, **kwargs):
        # Read from the table's JSON file all the rows in the current table
        # and return only the ones that match with provided arguments.

        with open(self.table_filepath, 'r') as fp:
            table_dict = json.load(fp)
            
            for row in table_dict['rows']:
                if all([row[k] == v for k,v in kwargs.items()]): 
                    yield Row(row)

    def all(self):
        # Similar to the `query` method, but simply returning all rows in
        # the table.
        with open(self.table_filepath, 'r') as fp:
            for row in json.load(fp)['rows']:
                yield Row(row)
    

    def count(self):
        # Read the JSON file and return the counter of rows in the table
        with open(self.table_filepath) as fp:
            return len(json.load(fp)['rows'])
            

    def describe(self):
        # Read the columns configuration from the JSON file, and return it.
        return self.columns
        
    def _serialize_date(self, arg):
        if isinstance(arg, date):
            return arg.isoformat()

class DataBase(object):
    def __init__(self, name):
        self.name = name
        self.db_filepath = os.path.join(BASE_DB_FILE_PATH, self.name)
        self.tables = self._read_tables()
        

    @classmethod
    def create(cls, name):
        db_filepath = os.path.join(BASE_DB_FILE_PATH, name)
        # if the db directory already exists, raise ValidationError
        # otherwise, create the proper db directory
        if os.path.exists(db_filepath):
            raise ValidationError('Database with name "{}" already exists.'.format(name))
        os.makedirs(db_filepath)

    def _read_tables(self):
        # Gather the list of tables in the db directory looking for all files
        # with .json extension.
        file_list = []
        for file in os.listdir(self.db_filepath):
            if file.endswith(".json"):
                file_list.append(file.replace('.json', ''))
            
            for f in file_list:
                setattr(self, f,Table(db=self, name=f))
        return file_list


    def create_table(self, table_name, columns):
        # Check if a table already exists with given name. If so, raise
        # ValidationError exception.

        if table_name in self.tables:
            raise ValidationError("Table with name '{}' already exists".format(table_name))
        table = Table(db=self,name=table_name, columns=columns)
        self.tables.append(table_name)
        setattr(self, table_name, table)
        

    def show_tables(self):
        # Return the current list of tables.
        return self.tables


def create_database(db_name):
    """
    Creates a new DataBase object and returns the connection object
    to the brand new database.
    """
    DataBase.create(db_name)
    return connect_database(db_name)


def connect_database(db_name):
    """
    Connectes to an existing database, and returns the connection object.
    """
    return DataBase(name=db_name)
