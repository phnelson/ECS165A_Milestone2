from template.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.max_records = PAGE_SIZE / COL_DATA_SIZE
        self.data = bytearray(PAGE_SIZE)

    def hasCapacity(self):
        if self.num_records < self.max_records:
            return True
        else:
            return False

    def read(self, offset):
        #Reads the value starting at offset for COL_DATA_SIZE bytes into value
        value = self.data[offset:offset+COL_DATA_SIZE]

        ret_value = int.from_bytes(value, byteorder='little')
        return ret_value

    def write(self, value):
        #Starting index for new entry saved as offset
        offset = self.num_records * COL_DATA_SIZE
        #Write data into page starting at offset for COL_DATA_SIZE bytes
        #del self.data[offset:offset+8]
        replace = value.to_bytes(COL_DATA_SIZE, byteorder='little')
        self.data[offset:offset+COL_DATA_SIZE] = replace
        #Increment the num_record count for page
        self.num_records += 1
        #Return offset to caller
        return offset

    def edit(self, offset, value):
        #Edits the value starting at offset for COL_DATA_SIZE bytes to the new value
        replace = value.to_bytes(COL_DATA_SIZE, byteorder='little')
        self.data[offset:offset+COL_DATA_SIZE] = replace

class PageBlock:

    def __init__(self, num_columns):
        #Calculate the total number of pages in the block
        self.total = (num_columns + METADATA_COLS)
        #Move config.py constants into object memory
        self.indir = INDIRECTION_COLUMN
        self.rid = RID_COLUMN
        self.time = TIMESTAMP_COLUMN
        self.schema = SCHEMA_ENCODING_COLUMN
        self.data_start = METADATA_COLS
        #Vector for pages, and set constants for current capacity and max capacity
        self.pages = []
        self.entry_count = 0
        self.entry_max = PAGE_SIZE // COL_DATA_SIZE

        #Initialize and add self.total pages into pages vector
        for i in range(self.total):
            self.pages.append(Page())

    def hasCapacityEntry(self):
        #Checks capacity of a page in a page, as all pages will have the same number of entries
        return self.pages[self.indir].hasCapacity()

    def getIndirection(self, offset):
        #Returns the indirection value of the record at the offset
        return self.pages[self.indir].read(offset)

    def getNextOffset(self):
        val = self.pages[self.rid].num_records
        return val

    def readCol(self, index, offset):
        val = self.pages[index].read(offset)
        print(val)
        return val

    def writeCol(self, index, value):
        offset = self.pages[index].write(value)
        return (index*self.entry_max) + offset

    def editCol(self, index, offset, value):
        self.pages[index].edit(offset, value)

    def deleteRecord(self, offset):
        self.pages[self.rid].edit(offset, 0)

class PageRange:

    def __init__(self, num_columns):
        #Calculate the total number of pages in the range
        self.total = BASE_CONST + TAIL_CONST
        #Vector for pages, and set constants for current capacity and max capacity
        self.num_columns = num_columns
        self.page_blocks = []
        self.base_count = 0
        self.tail_count = 0
        self.max_base = BASE_CONST
        self.max_tail = TAIL_CONST

        #Initialize and add self.total pages into pages vector
        for i in range(self.total):
            self.page_blocks.append(PageBlock(self.num_columns))
    
    def hasCapacityBase(self):
        #Checks working page_block[base_count] for capacity
        if self.page_blocks[self.base_count].hasCapacityEntry() == False :
            #If no capacity, itterate to next base page_block
            self.base_count += 1
            #Checks that new base page_block index does not exceed bounds
            if self.base_count == self.max_base:
                #Returns False if bound exceeded, new PageRange required
                return False
            else:
                #Bound not exceeded, redundant check for capacity
                return self.hasCapacityBase()
        else:
            #Current working page_block[base_count] has capacity
            return True
            
    def hasCapacityTail(self):
        #Checks working page_block[tail_count + max_base] for capacity
        if self.page_blocks[self.tail_count + self.max_base].hasCapacityEntry == False :
            #If no capacity, itterate to next tail page_block
            self.tail_count += 1
            #Checks that new tail page_block index does not exceed bounds
            if self.tail_count == self.max_tail:
                #Returns False if bound exceeded, new PageRange required
                return False
            else:
                #Bound not exceeded, redundant check for capacity
                return self.hasCapacityTail()
        else:
            #Current working page_block[base_count] has capacity
            return True

    def getIndirection(self, pageBlock, offset):
        value = self.page_blocks[pageBlock].getIndirection(offset)
        print(value)
        return value

    def nextBaseRid(self):
        prerid = self.page_blocks[self.base_count].getNextOffset()
        rid = ((self.base_count) * (PAGE_SIZE // COL_DATA_SIZE)) + prerid
        return rid

    def nextTailRid(self):
        prerid = self.page_blocks[self.tail_count].getNextOffset()
        rid = ((self.tail_count) * (PAGE_SIZE // COL_DATA_SIZE)) + prerid
        return rid

    def readBlock(self, pageBlock, offset):
        readBlock = []
        #For all columns in page_block[pageBlock]
        for index in range(self.page_blocks[pageBlock].total):
            #Read column i at ofset, append to readBlock
            readBlock.append(self.page_blocks[pageBlock].readCol(index, offset))
            #print(readBlock[index])
        print(index)
        
        return readBlock

    def writeBaseBlock(self, columns):
        #Writes the index data column into index page of the block
        for index in range(self.page_blocks[self.base_count].total):
            #Write columns[index] into block pageBlock, page index, data[index]
            self.page_blocks[self.base_count].writeCol(index, columns[index])

    def writeTailBlock(self, *columns):
        #Writes the first data column into first page of the block, recording the offset
        offset = self.page_blocks[self.tail_count].writeCol(0, columns[0])
        #For all remaining columns
        for index in range(1,self.total):
            #Write columns[index] into block pageBlock, page index, data[index]
            self.page_blocks[self.tail_count].writeCol(index, columns[index])

        return offset

    def editBlock(self, pageBlock, index, offset, value):
        self.page_blocks[pageBlock].editCol(index, offset, value)
   
    def deleteRecord(self, pageBlock, offset):
        self.page_blocks[pageBlock].deleteRecord(offset)