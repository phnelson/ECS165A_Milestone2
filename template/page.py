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
        # Reads the value starting at offset for COL_DATA_SIZE bytes into value
        tru_offset = offset * COL_DATA_SIZE
        value = self.data[tru_offset:tru_offset + COL_DATA_SIZE]
        ret_value = int.from_bytes(value, byteorder='little')
        return ret_value

    def write(self, value):
        # Starting index for new entry saved as offset
        offset = self.num_records * COL_DATA_SIZE
        # Convert value into bytes
        replace = value.to_bytes(COL_DATA_SIZE, byteorder='little')
        # Write data into page starting at offset for COL_DATA_SIZE bytes
        self.data[offset:offset + COL_DATA_SIZE] = replace
        # Increment the num_record count for page
        self.num_records += 1
        # Return offset to caller
        return offset

    def edit(self, offset, value):
        # Edits the value starting at offset for COL_DATA_SIZE bytes to the new value
        # Calculates the true offset for inserting data
        tru_offset = offset * COL_DATA_SIZE
        # Convert value into bytes
        replace = value.to_bytes(COL_DATA_SIZE, byteorder='little')
        # Overrite data into page starting at offset for COL_DATA_SIZE bytes
        self.data[tru_offset:tru_offset + COL_DATA_SIZE] = replace


class PageBlock:

    def __init__(self, num_columns):
        # Calculate the total number of pages in the block
        self.total = (num_columns + METADATA_COLS)
        # Move config.py constants into object memory
        self.indir = INDIRECTION_COLUMN
        self.rid = RID_COLUMN
        self.time = TIMESTAMP_COLUMN
        self.schema = SCHEMA_ENCODING_COLUMN
        self.data_start = METADATA_COLS
        # Vector for pages, and set constants for current capacity and max capacity
        self.pages = []
        self.entry_count = 0
        self.entry_max = PAGE_SIZE // COL_DATA_SIZE

        # Initialize and add self.total pages into pages vector
        for i in range(self.total):
            self.pages.append(Page())

    def hasCapacityEntry(self):
        # Checks capacity of a page in a page, as all pages will have the same number of entries
        return self.pages[self.indir].hasCapacity()

    def getIndirection(self, offset):
        # Returns the indirection value of the record at the offset
        return self.pages[self.indir].read(offset)

    def getNextOffset(self):
        val = self.pages[self.rid].num_records
        return val

    def readCol(self, index, offset):
        val = self.pages[index].read(offset)
        return val

    def writeCol(self, index, value):
        offset = self.pages[index].write(value)
        return (index * self.entry_max) + offset

    def editCol(self, index, offset, value):
        self.pages[index].edit(offset, value)

    def deleteRecord(self, offset):
        self.pages[self.rid].edit(offset, 0)


class PageRange:

    def __init__(self, num_columns):
        # Calculate the total number of pages in the range
        self.total = BASE_CONST + TAIL_CONST
        # Vector for pages, and set constants for current capacity and max capacity
        self.num_columns = num_columns
        self.page_blocks = []
        self.base_count = 0
        self.tail_count = 0
        self.max_base = BASE_CONST
        self.max_tail = TAIL_CONST

        # Initialize and add self.total pages into pages vector
        for i in range(self.total):
            self.page_blocks.append(PageBlock(self.num_columns))

    def hasCapacityBase(self):
        # Checks working page_block[base_count] for capacity
        if self.page_blocks[self.base_count].hasCapacityEntry() == False:
            # If no capacity, iterate to next base page_block
            self.base_count += 1
            # Checks that new base page_block index does not exceed bounds
            if self.base_count >= self.max_base:
                # Returns False if bound exceeded, new PageRange required
                return False
            else:
                # Bound not exceeded, redundant check for capacity
                return self.hasCapacityBase()
        else:
            # Current working page_block[base_count] has capacity
            return True

    def hasCapacityTail(self):
        # Checks working page_block[tail_count + max_base] for capacity
        if self.page_blocks[self.tail_count + self.max_base].hasCapacityEntry() == False:
            # If no capacity, iterate to next tail page_block
            self.tail_count += 1
            # Checks that new tail page_block index does not exceed bounds
            if self.tail_count >= self.max_tail:
                # Returns False if bound exceeded, new PageRange required
                return False
            else:
                # Bound not exceeded, redundant check for capacity
                return self.hasCapacityTail()
        else:
            # Current working page_block[base_count] has capacity
            return True

    # Gets the indirection value stored in the record at page_Block and offset
    def getIndirection(self, page_Block, offset):
        value = self.page_blocks[page_Block].getIndirection(offset)
        return value

    # Step calculation in determining the rid of the next available base record
    def nextBaseRid(self):
        # Gets the prior part of the rid calculation from the current base PageBlock
        prerid = self.page_blocks[self.base_count].getNextOffset()
        # Computes this step in the rid calculation and adds on the previous steps calculation
        rid = (self.base_count * (PAGE_SIZE // COL_DATA_SIZE)) + prerid
        return rid

    # Step calculation to determining the rid of the next available tail record
    def nextTailRid(self):
        # Gets the prior part of the rid calculation from the current tail PageBlock
        prerid = self.page_blocks[self.tail_count + self.max_base].getNextOffset()
        # Computes this step in the rid calculation and adds on the previous steps calculation
        rid = ((self.tail_count + self.max_base) * (PAGE_SIZE // COL_DATA_SIZE)) + prerid
        return rid

    # Reads the metaData + data columns of a record
    def readBlock(self, page_Block, offset):
        read_Block = []
        # For all columns in page_block[pageBlock]
        for index in range(self.page_blocks[page_Block].total):
            # Read column i at ofset, append to readBlock
            read_Block.append(self.page_blocks[page_Block].readCol(index, offset))
        # Return compiled readBlock
        return read_Block

    # Writes the metaData + data columns of a concurrent record to a base record
    def writeBaseBlock(self, columns):
        # Write columns[index] into block pageBlock, page index, data[index]
        for index in range(self.page_blocks[self.base_count].total):
            self.page_blocks[self.base_count].writeCol(index, columns[index])

    # Writes the metaData + data columns of a concurrent record to a tail record
    def writeTailBlock(self, columns):
        # Write columns[index] into PageBlock, page index, data[index]
        for index in range(self.page_blocks[self.tail_count + self.max_base].total):
            self.page_blocks[self.tail_count + self.max_base].writeCol(index, columns[index])

    # Edits/replaces the value at a given page_block and offset, at the index of the record
    def editBlock(self, page_Block, index, offset, value):
        self.page_blocks[page_Block].editCol(index, offset, value)

    # Deletes the record at a given page_block and offset
    def deleteRecord(self, page_Block, offset):
        self.page_blocks[page_Block].deleteRecord(offset)
