"""
A data structure holding indices for various columns of a table.
Key column should be indexed by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
import pickle

from template.config import *
from template.page import *


class Index:

    def __init__(self, table, num_columns):
        # One index for each table. All our empty initially.
        self.indices = [None] * num_columns
        self.num_columns = num_columns
        self.table = table
        # Populate index for key column
        pass

    def getIndex(self, column_num):
        return self.indices[column_num]

    def deletePair(self, column_num, key, value):
        index = self.indices[column_num]
        if index is None:
            index = dict()

        index[key].remove(value)
        self.indices[column_num] = index

    def insertPair(self, column_num, key, value):

        #print("col, key, val:", column_num, key, value)
        index = self.indices[column_num]

        if index is None:
            index = dict()

        if key not in index:
            index[key] = []

        index[key].append(value)

        self.indices[column_num] = index

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        #print("test2:", self.table.getIndex(column))
        dic = self.table.getIndex(column)
        #print(dic)
        ret_val = dic.get(value)
        return ret_val
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):

        ret_val = []
        dic = self.table.getIndex(column)

        for test in range(begin, end):
            test_val = dic.get(test)
            if test_val is None:
                pass
            else:
                ret_val = ret_val + test_val

        return ret_val


    """
    # optional: Create index on specific column
    """
    def create_index(self, column_number):
        # loop through all base blocks that are not historic
        new_index = dict()

        # print("Creating index!")

        for pageR in range(0, self.table.curr_page_range + 1):
            for pageB in range(0, BASE_CONST):
                for offset in range (0, PAGE_SIZE // COL_DATA_SIZE):

                    rid = self.table.getRID(pageR, pageB, offset)
                    record = self.table.readRecord(rid)

                    if record is not None:
                        cols = record.getColumns()
                        data_val = cols[column_number]
                        if data_val not in new_index:
                            new_index[data_val] = []
                        new_index[data_val].append(rid)

        if self.indices[column_number] is not None:
            self.indices[column_number].clear()


        self.indices[column_number] = new_index

    '''
    def create_index(self, column_number):
        # loop through all base blocks that are not historic
        new_index = {}

        print("Creating index!")

        for pageR in range(0, self.table.curr_page_range + 1):

            print("Working pageR: ", pageR)
            with open('%d.prange' % pageR, 'rb') as f:
                working_range = pickle.load(f)

            if working_range is None:
                print("working range not found at pageR = ", self.table.curr_page_range + 1)
                return
            else:
                pass

            if working_range.getHistoric():
                pass
            else:
                print("Working range base count:", working_range.base_count)

                for block in range(0, working_range.base_count + 1):
                    block_entries_plus1 = working_range.page_blocks[block].getNextOffset() + 1
                    for offset in range(0, block_entries_plus1 ):
                        full_record = working_range.readBlock(block, offset)
                        if full_record[RID_COLUMN] != 0:
                            data_record = full_record[len(full_record) - self.num_columns:]
                            print("Datarecord: ", data_record)
                            if data_record[column_number] == 0:
                                pass
                            else:
                                data_val = data_record[column_number]
                                # new_index[data_val].append(full_record[RID_COLUMN])
                                new_index[data_val] = full_record[RID_COLUMN]

        if self.indices[column_number] is not None:
            self.indices[column_number].clear()

        self.indices[column_number] = new_index
    '''

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):

        if self.indices[column_number] is not None:
            self.indices[column_number].clear()
            self.indices[column_number] = None
        else:
            pass

