from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades

total_Loops = 10000

db = Database()
db.open('ECS165')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, total_Loops):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()

print("Inserting ", total_Loops, " records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [randrange(0, 100), None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, total_Loops):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating ", total_Loops, " records took:  \t\t\t", update_time_1 - update_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, total_Loops):
    query.select(choice(keys), 0, [1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting ", total_Loops, " records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, total_Loops, 100):
    result = query.sum(i, 100, randrange(0, 5))
agg_time_1 = process_time()
print("Aggregate ", total_Loops, " of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, total_Loops):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting ", total_Loops, " records took:  \t\t\t", delete_time_1 - delete_time_0)
db.close()
