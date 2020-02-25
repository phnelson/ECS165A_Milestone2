from template.page import *
from template.buffer_range import *
from template.index import Index
from time import time
import sys


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    # Getter for columns
    def getColumns(self):
        return self.columns

    # to_string() function that returns the data of Record
    def __str__(self):
        return str(self.columns)


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # Replace with index, and all references inside table and query with index API
        # self.index = Index(self, self.num_columns)
        self.buffer_pool_range = BufferPoolRange(BUFFER_POOL_SIZE_RANGE, num_columns)
        # self.page_ranges = []
        # self.page_ranges.append(PageRange(self.num_columns))
        self.curr_page_range = 0

    # Future function to merge tail records into base records
    def __merge(self):
        pass

    # Creates a new PageRange if needed, and appends it to page_ranges
    def newPageRange(self):
        # self.page_ranges.append(PageRange(self.num_columns))
        # self.buffer_pool_range.
        self.curr_page_range = self.curr_page_range + 1

    # Helper function for the translation of RID value to RID components
    def getOffset(self, rid):
        return rid % (PAGE_SIZE // COL_DATA_SIZE)

    # Helper function for the translation of RID value to RID components
    def getPageR(self, rid):
        return rid // ((BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE))

    # Helper function for the translation of RID value to RID components
    def getPageB(self, rid):
        return (rid // (PAGE_SIZE // COL_DATA_SIZE)) % (BASE_CONST + TAIL_CONST)

    # Helper function for the translation of RID components to RID value
    def getRID(self, pageR, pageB, offset):
        return (pageR * (BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE)) + (
                    pageB * (PAGE_SIZE // COL_DATA_SIZE)) + offset

    # Helper function to find the value of the next RID before writing to basepages
    def nextBaseRid(self):
        # Calls for calculation of the first two RID components
        # #prerid = self.page_ranges[self.curr_page_range].nextBaseRid()
        prerid = self.buffer_pool_range.nextBaseRid_Pool(self.curr_page_range)
        # Calculates the last RID component and adds it together with the previous for the next base RID
        rid = self.curr_page_range * (BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE) + prerid
        return rid

    # Helper function to find the value of the next tail RID before writing to tail pages
    def nextTailRid(self):
        # Calls for calculation of the first two RID components
        # #prerid = self.page_ranges[self.curr_page_range].nextTailRid()
        prerid = self.buffer_pool_range.nextTailRid_Pool(self.curr_page_range)
        # Calculates the last RID component and adds it together with the previous for the next tail RID
        rid = self.curr_page_range * (BASE_CONST + TAIL_CONST) * (PAGE_SIZE // COL_DATA_SIZE) + prerid
        return rid

    # Helper function unique for this metadata scheme
    def formatCols(self, indir, rid, timestamp, schema, columns):
        format_cols = []
        format_cols.append(indir)
        format_cols.append(rid)
        format_cols.append(timestamp)
        format_cols.append(schema)

        for index in range(self.num_columns):
            format_cols.append(columns[index])

        return format_cols

    # Function to set the RID of a record to the invalid value
    def deleteRecord(self, rid):
        pageR = self.getPageR(rid)
        pageB = self.getPageB(rid)
        offset = self.getOffset(rid)
        # #self.page_ranges[pageR].deleteRecord(pageB, offset)
        self.buffer_pool_range.deleteRecord_Pool(pageR, pageB, offset)

    # Function to check the indirection value of a record before doing a full read
    def checkIndirection(self, rid):
        pageR = self.getPageR(rid)
        pageB = self.getPageB(rid)
        offset = self.getOffset(rid)
        # print(pageR, pageB, offset)
        # #indir = self.page_ranges[pageR].getIndirection(pageB, offset)
        indir = self.buffer_pool_range.getIndirection_Pool(pageR, pageB, offset)

        if indir == 0:
            return rid
        else:
            return indir

    #
    def readRecord(self, rid):
        # Gets the true rid for the most recent version of the data
        trueRID = self.checkIndirection(rid)
        # Does the math to calculate pageR, pageB, and offset for record retrieval
        pageR = self.getPageR(trueRID)
        pageB = self.getPageB(trueRID)
        offset = self.getOffset(trueRID)

        # print("RID:",rid)
        # print("TrueRID:",trueRID)
        # print("Reading: Rid=",rid," pageR=",pageR," pageB=",pageB," offset=",offset)

        # Retrieves record
        # #full_record = self.page_ranges[pageR].readBlock(pageB, offset)
        full_record = self.buffer_pool_range.readBlock_Pool(pageR, pageB, offset)
        data_record = full_record[len(full_record) - self.num_columns:]

        # print(full_record)

        ret_record = Record(rid, data_record[self.key], data_record)
        return ret_record

    def insertRecord(self, columns):
        # Check for room for base page, if not make more room
        # #if self.page_ranges[self.curr_page_range].hasCapacityBase() == False:
        if self.buffer_pool_range.hasCapacityBase_Pool(self.curr_page_range) == False:
            self.newPageRange()

        indir = 0
        schema_encoding = 0  # '0' * self.num_columns
        cur_Time = 0  # time()
        base_rid = self.nextBaseRid()
        format_columns = self.formatCols(indir, base_rid, cur_Time, schema_encoding, columns)
        # print(format_columns)
        # #self.page_ranges[self.curr_page_range].writeBaseBlock(format_columns)
        self.buffer_pool_range.writeBaseBlock_Pool(self.curr_page_range, format_columns)

        return base_rid

    def updateRecord(self, rid, columns):
        # Check for room for tail page, if not make more room
        # #if self.page_ranges[self.curr_page_range].hasCapacityTail() == False:
        if self.buffer_pool_range.hasCapacityTail_Pool(self.curr_page_range) == False:
            self.newPageRange()

        page_R = self.getPageR(rid)
        page_B = self.getPageB(rid)
        page_offset = self.getOffset(rid)

        # #prev_vers = self.page_ranges[page_R].getIndirection(page_B, page_offset)
        prev_vers = self.buffer_pool_range.getIndirection_Pool(page_R, page_B, page_offset)
        schema_encoding = 0  # '0' * self.num_columns
        currTime = 0  # time()
        tail_rid = self.nextTailRid()

        prev_record = self.readRecord(rid)
        prev_columns = prev_record.getColumns()

        # print(prev_columns)
        new_columns = []

        for index in range(self.num_columns):
            if type(columns[index]) == type(None):
                new_columns.append(prev_columns[index])
            else:
                new_columns.append(columns[index])

        # print(new_columns)

        format_columns = self.formatCols(prev_vers, tail_rid, currTime, schema_encoding, new_columns)

        # print(format_columns)
        # #self.page_ranges[self.curr_page_range].writeTailBlock(format_columns)
        self.buffer_pool_range.writeTailBlock_Pool(self.curr_page_range, format_columns)

        # #self.page_ranges[page_R].editBlock(page_B, INDIRECTION_COLUMN, page_offset, tail_rid)
        self.buffer_pool_range.editBlock_Pool(page_R, page_B, INDIRECTION_COLUMN, page_offset, tail_rid)
