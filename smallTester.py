from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# Measuring Insert Performance


query.insert(906659671, 93, 5, 7, 0)
print(query.select(906659671, [1,1,1,1,1])[0])
    