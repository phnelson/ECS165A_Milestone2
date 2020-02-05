from template.page import *
from time import time
import sys

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def getColumns(self):
      return self.columns

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
        self.page_directory = {}
        self.page_ranges = []
        self.page_ranges.append(PageRange(self.num_columns))

        self.curr_page_range = 0
        pass

    def __merge(self):
        pass
 
    def newPageRange(self):
        self.page_ranges.append(PageRange(self.num_columns))
        self.curr_page_range = self.curr_page_range + 1

    def getOffset(self, rid):
        return rid % (PAGE_SIZE // COL_DATA_SIZE)

    def getPageR(self, rid):
        return (rid // ((BASE_CONST + TAIL_CONST)*(PAGE_SIZE // COL_DATA_SIZE)))

    def getPageB(self, rid):
        return ((rid // (PAGE_SIZE // COL_DATA_SIZE)) % (BASE_CONST + TAIL_CONST))

    def getRID(self, pageR, pageB, offset):
        return (pageR*(BASE_CONST + TAIL_CONST)*(PAGE_SIZE // COL_DATA_SIZE)) + (pageB*(PAGE_SIZE // COL_DATA_SIZE)) + offset

    def nextBaseRid(self):
        prerid = self.page_ranges[self.curr_page_range].nextBaseRid()
        rid = (self.curr_page_range)*(BASE_CONST + TAIL_CONST)*(PAGE_SIZE // COL_DATA_SIZE) + prerid
        return rid

    def nextTailRid(self):
        prerid = self.page_ranges[self.curr_page_range].nextTailRid()
        rid = (self.curr_page_range)*(BASE_CONST + TAIL_CONST)*(PAGE_SIZE // COL_DATA_SIZE) + prerid
        return rid

    #Helper function unique for this metadata scheme
    def formatCols(inder, rid, timestamp, schema, *columns):
        format_cols = []
        format_cols.append(inder)
        format_cols.append(rid)
        format_cols.append(time)
        format_cols.append(schema)

        for index in range(len(columns)):
          format_cols.append(columns[index])

        return format_cols

    def deleteRecord(self, rid):
        pageR = self.getPageR(rid)
        pageB = self.getPageB(rid)
        offset = self.getOffset(rid)
        #print("Calculate: Rid=",self.getRID(pageR,pageB,offset))
        #print("Delete: Rid=",rid," pageR=",pageR," pageB=",pageB," offset=",offset)
        self.page_ranges[pageR].deleteRecord(pageB, offset)

    def insertRecord(self, *columns):
        #Check for room for base pase, if not make more room
        if self.page_ranges[self.curr_page_range].hasCapacityBase() == False :
            self.newPageRange()
        
        schema_encoding = '0' * self.num_columns
        currTime = time()
        base_rid = self.nextBaseRid()
        #format_columns = self.formatCols(None, base_rid, currTime, schema_encoding, columns)
        format_columns = [0, 6, 7, 5, 3, 0, 9, 1, 8]
        #print(columns)
        self.page_ranges[self.curr_page_range].writeBaseBlock(format_columns)

        return base_rid

    def updateRecord(self, rid, *columns):
        #Check for room for tail page, if not make more room
        if self.page_ranges[self.curr_page_range].hasCapacityTail() == False:
            self.newPageRange()

        page_R = self.getPageR(rid)
        page_B = self.getPageB(rid)
        page_offset = self.getOffset(rid)

        indirect = self.page_ranges[page_R].getIndirection(page_B, page_offset)
        schema_encoding = '0' * self.num_columns
        currTime = time()
        tail_rid = self.nextTailRid()

        format_columns = self.formatCols(indirect, tail_rid, currTime, schema_encoding, columns)
        
        self.page_ranges[self.curr_page_range].writeTailBlock(columns)

        self.page_ranges[page_R].editBlock(page_B, INDIRECTION_COLUMN, page_offset, tail_rid)
    
    def checkIndirection(self, rid):
        pageR = self.getPageR(rid)
        pageB = self.getPageB(rid)
        offset = self.getOffset(rid)
        print(pageR, pageB, offset)
        indir = self.page_ranges[pageR].getIndirection(pageB, offset)

        if indir == -1:
            return rid
        else:
            return indir

    def readRecord(self,rid):
        #Gets the true rid for the most recent version of the data 
        trueRID = self.checkIndirection(rid)
        #Does the math to calculate pageR, pageB, and offset for record retrieval
        pageR = self.getPageR(trueRID)
        pageB = self.getPageB(trueRID)
        offset = self.getOffset(trueRID)
        print("RID:",rid)
        print("TrueRID:",trueRID)
        print("Read: Rid=",rid," pageR=",pageR," pageB=",pageB," offset=",offset)
        #Retrieves record
        
        record = self.page_ranges[pageR].readBlock(pageB, offset)

        print(record)
        return record