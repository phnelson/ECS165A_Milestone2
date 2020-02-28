from template.table import Table
from os import path
from os import chdir
from os import getcwd
import pickle


class Database():

    def __init__(self):
        self.tables = []
        self.prev_path = None
        pass

    def __str__(self):
        ret_val = []
        for table in range(0, len(self.tables)):
            ret_val.append(str(self.tables[table]))
        return str(ret_val)

    #'''
    def open(self, my_path):
        if path.exists(my_path):
            self.prev_path = getcwd()
            chdir(my_path)
            if path.exists('database.database'):
                with open('database.database', 'rb') as f:
                    self.tables = pickle.load(f)
            else:
                pass
        else:
            print(my_path, " not found...")
            pass

    '''
    def open(self):
        if path.exists('database.database'):
            with open('database.database', 'rb') as f:
                self.tables = pickle.load(f)
        else:
            pass
    #'''

    def close(self):
        for i in range(0, len(self.tables)):
            self.tables[i].close()

        with open('database.database', 'wb') as f:
            pickle.dump(self.tables, f, pickle.HIGHEST_PROTOCOL)

        chdir(self.prev_path)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        for x in range(0, len(self.tables)):
            if self.tables[x].getName() == name:
                print("Table with that name already exists! Choose another name or drop table: ", name)
                exit()
                return None
            else:
                pass
        table = Table(name, num_columns, key)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        pass

    """
        # Returns table with the passed name
        """

    def get_table(self, name):
        for x in range(0, len(self.tables)):
            if self.tables[x].getName() == name:
                return self.tables[x]
            else:
                pass

        return None
