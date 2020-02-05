from template.table import Table, Record
from template.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        rid = self.table.page_directory.get(key)
        #if rid == None:
        #    return None
        #else:
        #go into memory with the rid -> (PageRange, offset), change value of Record.rid to None
        self.table.deleteRecord(rid)
        #remove key from dictionary ,dict.pop(key) might be useful
        self.table.page_directory.pop(key, None)

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        key = columns[self.table.key]
        #Insert value into available base -> get rid->(page, offset)
        rid = self.table.insertRecord(columns)
        #Add key,rid pair to dictionary
        self.table.page_directory[key] = rid

    """
    # Read a record with specified key
    """

    def select(self, key, *query_columns):
        rid = self.table.page_directory.get(key)
        #Go into memory and read the value stored at rid->indirection
        record = self.table.readRecord(rid)
        returnRecord = []
        #loops through range(len(query_columns)
        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                #read value into temp structure
                returnRecord.append(record[i])
                #returnRecord.append(None) ???
        #returns temp structure of read values
        return returnRecord
                
    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
      schema_encoding = '0' * self.table.num_columns
      key = columns[self.table.key]
      rid = self.table.page_directory.get(key)
      if rid != None:
          #go into memory with the rid, update next available tail record and link rid->inderection 
          self.table.updateRecord(rid, columns)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        sum_val = 0
        selected_cols = []
        data = []

        num_cols = self.table.num_columns

        for i in range(0, num_cols):
          if i == aggregate_column_index:
            selected_cols.append(1)
          else: 
            selected_cols.append(0)

        for i in (start_range, end_range+1):
          data = self.select(i, selected_cols)

          sum_val += data[0].get_columns()[aggregate_column_index]
        
        return sum_val