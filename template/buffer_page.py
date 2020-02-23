from template.page import *
class BufferPage:

    def __init__(self, pid, page):
        self.pid = pid
        self.dirty = 0
        self.pin = 0
        self.page_data = page

    def pin(self):
        self.pin += 1

    def unpin(self):
        self.pin -= 1

    def delete(self):
        self.dirty = 0
        self.pin = 0
        self.page_data = None

    def getPage(self):
        return self.page_data

    def read(self, offset):
        self.page_data.read(offset)

    def write(self, value):
        self.page_data.write(value)
        self.dirty = 1

    def edit(self, offset, value):
        self.page_data.edit(offset, value)
        self.dirty = 1

class BufferPoolPage:

    def __init__(self, buffer_size):
        self.buffer_size = buffer_size
        self.buffer_pages = {}
        self.next_available = 0 #[0, buffer_size - 1]
        self.MRU = 0

    def loadPage(self, pid):
        pass

    def evictPage(self):
        # if self.buffer_pages[self.MRU].dirty
            # write page back to disk @ offset
        pass
    def getPage(self, pid):
        pass

    def readPage(self, pid, offset):
        pass
    def writePage(self, pid, value):
        pass
    def editPage(self, pid, offset, value):
        pass

