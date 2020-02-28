from os import path

from template.page import *
import pickle
import os.path
class BufferRange:

    def __init__(self):
        self.pageR = None
        self.dirty = 0
        self.pin = 0
        self.range_data = None # THIS IS A PAGERANGE CLASS

    def pin(self):
        self.pin += 1

    def unpin(self):
        self.pin -= 1

    def setDirty(self):
        self.dirty = 1

    def getPin(self):
        return self.pin

    def writeBack(self):
        if self.dirty == 1:
            #print("Writeback: True")
            return True
        else:
            #print("Writeback: False")
            return False

    def canEvict(self):
        if self.pin == 0:
            return True
        else:
            return False

    def delete(self):
        self.pageR = None
        self.dirty = 0
        self.pin = 0
        self.range_data = None

    def getRange(self):
        return self.range_data

    def setRange(self, prange):
        self.range_data = prange

    def getPageR(self):
        return self.pageR

    def setPageR(self, pageR):
        self.pageR = pageR

    def hasCapacityBase_Range(self):
        return self.range_data.hasCapacityBase()

    def hasCapacityTail_Range(self):
        return self.range_data.hasCapacityTail()

    def getIndirection_Range(self, page_block, offset):
        return self.range_data.getIndirection(page_block, offset)

    def nextBaseRid_Range(self):
        return self.range_data.nextBaseRid()

    def nextTailRid_Range(self):
        return self.range_data.nextTailRid()

    def readBlock_Range(self, page_block, offset):
        return self.range_data.readBlock(page_block, offset)

    def writeBaseBlock_Range(self, columns):
        return self.range_data.writeBaseBlock(columns)

    def writeTailBlock_Range(self, columns):
        return self.range_data.writeTailBlock(columns)

    def editBlock_Range(self, page_block, index, offset, value):
        return self.range_data.editBlock(page_block, index, offset, value)

    def deleteRecord_Range(self, page_block, offset):
        return self.range_data.deleteRecord(page_block, offset)

class BufferPoolRange:

    def __init__(self, buffer_size, num_columns):
        self.MRU = None # range from [0:buffer_size]
        self.num_columns = num_columns
        self.buffer_size = buffer_size
        self.buffer_dic = {}
        self.buffer_ranges = [] # range from [0:buffer_size]

        for i in range(0, buffer_size):
            self.buffer_ranges.append(BufferRange())

        self.next_available = 0 #[0, buffer_size - 1]

    def pageRToFileName(self, pageR):
        return '%d.prange' % pageR

    def evictAll(self):
        # for all slots in buffer_pool
        # print("EvictAll: dic = ", self.buffer_dic)
        # print("dic size = ", len(self.buffer_dic))
        for curr_range in range(0, self.buffer_size):
            # if slot value is not empty
            if not self.buffer_ranges[curr_range].getPageR() is None:
                self.evictRange(curr_range)
            else:
                pass

    def evictRange(self, index):
        # loop until MRU is free to evict from pool
        loop = True
        curr_range = None

        while(loop):
            curr_range = self.buffer_ranges[index]
            loop = not curr_range.canEvict()

        if curr_range.writeBack():
            #print("WriteBack!")

            #with open(self.pageRToFileName(curr_range.getPageR()), 'wb') as output:
            # f = open('%d.prange' % curr_range.getPageR(), 'wb')
            # pickle.dump(curr_range.getRange(), f, pickle.HIGHEST_PROTOCOL)

            f = open('%d.prange' % curr_range.getPageR(), 'wb')
            # with open(self.pageRToFileName(curr_range.getPageR()), 'wb') as output:
            pickle.dump(curr_range.getRange(), f, pickle.HIGHEST_PROTOCOL)

            # Pickling contained within a with block, as it closes the file automatically after exiting the block.
            # # with open('%d.prange' % curr_range.getPageR(), 'wb') as f:
            # with open(self.pageRToFileName(curr_range.getPageR(), 'wb')) as f:
                # pickle.dump(f, pickle.HIGHEST_PROTOCOL)
                # # pickle.dump(f)
        # print(self.buffer_dic)
        # print("Evicting range:", self.buffer_ranges[index].getPageR())
        self.buffer_dic.pop(self.buffer_ranges[index].getPageR())
        curr_range.delete()
        return index

    def loadRange(self, pageR):
        #print("Range Loader")
        dirty = False
        if len(self.buffer_dic) >= self.buffer_size:
            index = self.evictRange(self.MRU)
        else:
            index = len(self.buffer_dic)

        curr_range = None
        if path.exists(self.pageRToFileName(pageR)):
            #with open(self.pageRToFileName(pageR), 'rb') as input:
            with open('%d.prange' % pageR, 'rb') as input:
                curr_range = pickle.load(input)
        else:
            curr_range = PageRange(self.num_columns)
            dirty = True

        self.buffer_ranges[index].setPageR(pageR)
        self.buffer_ranges[index].setRange(curr_range)
        if dirty:
            # self.buffer_ranges[index].writeBack()
            self.buffer_ranges[index].setDirty()

        #print("Loading range:", pageR)
        self.buffer_dic[pageR] = index
        #print(self.buffer_dic)
        return index

    def getRange(self, pageR):
        index = self.buffer_dic.get(pageR)
        if index is None:
            index = self.loadRange(pageR)
        return index

    def hasCapacityBase_Pool(self, pageR):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].hasCapacityBase_Range()
        self.MRU = index
        return ret_value

    def hasCapacityTail_Pool(self, pageR):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].hasCapacityTail_Range()
        self.MRU = index
        return ret_value

    def getIndirection_Pool(self, pageR, page_block, offset):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].getIndirection_Range(page_block, offset)
        self.MRU = index
        return ret_value

    def nextBaseRid_Pool(self, pageR):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].nextBaseRid_Range()
        self.MRU = index
        return ret_value

    def nextTailRid_Pool(self, pageR):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].nextTailRid_Range()
        self.MRU = index
        return ret_value

    def readBlock_Pool(self, pageR, page_block, offset):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].readBlock_Range(page_block, offset)
        self.MRU = index
        return ret_value

    def writeBaseBlock_Pool(self, pageR, columns):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].writeBaseBlock_Range(columns)
        self.buffer_ranges[index].setDirty()
        self.MRU = index
        return ret_value

    def writeTailBlock_Pool(self, pageR, columns):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].writeTailBlock_Range(columns)
        self.buffer_ranges[index].setDirty()
        self.MRU = index
        return ret_value

    def editBlock_Pool(self, pageR, page_block, arg_index, offset, value):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].editBlock_Range(page_block, arg_index, offset, value)
        self.buffer_ranges[index].setDirty()
        self.MRU = index
        return ret_value

    def deleteRecord_Pool(self, pageR, page_block, offset):
        index = self.getRange(pageR)
        ret_value = self.buffer_ranges[index].deleteRecord_Range(page_block, offset)
        self.MRU = index
        return ret_value