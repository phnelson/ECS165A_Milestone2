from template.page import *
import pickle
class BufferRange:

    def __init__(self, pid, range):
        self.pid = pid
        self.dirty = 0
        self.pin = 0
        self.range_data = range

    def pin(self):
        self.pin += 1

    def unpin(self):
        self.pin -= 1

    def delete(self):
        self.dirty = 0
        self.pin = 0
        self.range_data = None

    def getRange(self):
        return self.range_data

    def read(self, offset):
        self.page_data.read(offset)

    def write(self, value):
        self.page_data.write(value)
        self.dirty = 1

    def edit(self, offset, value):
        self.page_data.edit(offset, value)
        self.dirty = 1

class BufferPoolRange:

    def __init__(self, buffer_size):
        self.buffer_size = buffer_size
        self.buffer_range = {}
        self.next_available = 0 #[0, buffer_size - 1]
        self.MRU = None

    def pageRToFileName(self, pageR):
        string = pageR + ".pkl"
        return string

    def evictRange(self):
        if self.buffer
        # write page back to disk @ offset


    def loadRange(self, pageR):
        if len(self.buffer_range) < self.buffer_size:
            with open(pageRToFileName(pageR), 'wb') as input:
                curr_range = pickle.load(input)



    def getRange(self, pageR):
        curr_range = self.buffer_range.get(pageR)
        if curr_range is None:
            curr_range = self.loadRange(pageR)
        return curr_range

    def readRange(self, pid, offset):
        self.MRU = pid

    def writeRange(self, pid, value):
        self.MRU = pid

    def editRange(self, pid, offset, value):
        self.MRU = pid


