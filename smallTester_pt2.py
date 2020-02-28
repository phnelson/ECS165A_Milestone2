from template.db import Database
from template.query import Query
import os

db2 = Database()
db2.open("ECS165")
#db2.open()
print(db2)
g_table = db2.get_table('Grades')
q = Query(g_table)
rec3 = q.select(92106429, [1,1,1,1,1])[0]
print("Rec3 = ", rec3)
db2.close()