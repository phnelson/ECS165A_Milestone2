from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange
import sys

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# Measuring Insert Performance


query.insert(92106429, 93, 5, 7, 0)
query.insert(92106430, 8, 7, 6, 5)
rec = query.select(92106429, [1,1,1,1,1])[0]
rec2 = query.select(92106430, [1,1,1,1,1])[0]
print(rec)
print(rec2)
#print(rec.rid, rec.key, rec.columns)
#print(rec2.rid, rec2.key, rec2.columns)
#
print("Update Test:")
record = query.select(rec2.key, [1, 1, 1, 1, 1])[0]
updater = [None,None,100,None,None]
updater2 =[None,None,None,100,None]
query.update(92106429, *updater)
rec3 = query.select(92106429, [1,1,1,1,1])[0]
print(rec3)
query.update(92106429, *updater2)
rec4 = query.select(92106429, [1,1,1,1,1])[0]
print(rec4)
