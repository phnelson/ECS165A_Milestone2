"""
A data structure holding indices for various columns of a table.
Key column should be indexed by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
import pickle

from template.config import *


class Index:

    def __init__(self, table, num_columns):
        # One index for each table. All our empty initially.
        self.indices = [None] * num_columns
        # Populate index for key column
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        # loop through all base blocks that are not historic

        for pageR in self.table.curr_page_range:
            working_range = pickle.load('%d.prange' %pageR)
            #if working_range.getHistorical() == False:
            #    pass
            #else:
            #    for block in range(0,BASE_CONST):



        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
