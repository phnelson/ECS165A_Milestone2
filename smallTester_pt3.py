from template.db import Database
from template.query import Query
import os

db = Database()
db.open("ECS165")
print(db)
g_table = db.get_table('Grades')
q = Query(g_table)
rec3 = q.select(92106429, [1,1,1,1,1])[0]
print("Rec3 = ", rec3)
q.table.createIndex(0)
dic = q.table.getIndex(0)
print("Index :", dic)
db.close()