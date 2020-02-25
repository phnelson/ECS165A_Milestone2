from template.table import Table
from os import path
import pickle


class Database():

    def __init__(self):
        self.tables = []
        pass

    def __str__(self):
        return str(self.tables[0])

    #'''
    def open(self, my_path):

        print(path.exists(my_path))
        if path.exists(my_path):
            with open(my_path+'database.database', 'rb') as f:
                self.tables = pickle.load(f)
        else:
            pass

    '''
    def open(self):
        with open('database.database', 'rb') as f:
            self.tables = pickle.load(f)
    '''

    def close(self):
        for i in range(0, len(self.tables)):
            self.tables[i].close()

        with open('database.database', 'wb') as f:
            pickle.dump(self.tables, f, pickle.HIGHEST_PROTOCOL)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
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
