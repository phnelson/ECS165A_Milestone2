from template.table import Table, Record


# from template.index import Index

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
        # Go into memory with the rid -> (PageRange, offset), change value of Record.rid to None
        self.table.deleteRecord(rid)
        # Remove key from dictionary ,dict.pop(key) might be useful
        self.table.page_directory.pop(key, None)

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        key = columns[self.table.key]
        # Insert value into available base -> get rid->(page, offset)
        rid = self.table.insertRecord(columns)
        # print("Insert: rid = ", rid)
        # Add key,rid pair to dictionary
        self.table.page_directory[key] = rid

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        retList = []
        rid = self.table.page_directory.get(key)
        # Validator for record presence in table
        if rid is None:
            retList.append(None)
            return retList
        # Go into memory and read the value stored at rid or its indirection
        record = self.table.readRecord(rid)
        retList.append(record)
        return retList
    '''
    def select(self, key, column, query_columns):
        retList = []
        rids[] = self.table.index.locate(column, key)
        if len(rids) == 0:
            retList.append(None)
            return retList
        else:
            for rec in range(0,len(rids)):


        return retList
    '''

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        rid = self.table.page_directory.get(key)
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

        # Pre-processing to set up selected_cols for query.select() calls
        for i in range(0, num_cols):
            if i == aggregate_column_index:
                selected_cols.append(1)
            else:
                selected_cols.append(0)

        # Loops through the specified range
        for i in range(start_range, end_range + 1):
            data = self.select(i, selected_cols)[0]
            # Data validation to ensure it exists in table
            if data is not None:
                sum_val += data.getColumns()[aggregate_column_index]
            # print(sum_val)

        return sum_val
