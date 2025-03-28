import SimpleDB
from time import perf_counter

db = SimpleDB.SimpleDB("db.json")

# db.create_table("users", ["id", "name", "age", "word"])

# db.insert("users", {"id": "3", "name": "Bill", "age": 4})
# db.insert("users", {"id": "1", "name": "B", "age": 125, "word": "test"})
# db.insert("users", {"id": "2", "name": "Bi", "age": 23, "word": "test"})
# db.insert("users", {"id": "4", "name": "Bil", "age": 74, "word": "test"})
# db.insert("users", {"id": "5", "name": "Billl", "age": 14, "word": "test"})


# db.update("users", {"age": 4, "word": "test"}, where = {"age": {"gt": 25}})

# print(db.execute("SELECT name, age FROM users WHERE age > 25"))
# print(db.execute("INSERT INTO users (id, name, age) VALUES (3, Charlie, 22)"))
# print(db.execute("DELETE FROM users"))
# print(db.execute("UPDATE users SET name = 'BERRR', word = 'TEST', age = 300 WHERE age > 100"))
start_time = perf_counter()
print(db.execute("SELECT * FROM users WHERE  = 1"))
end_time = perf_counter()
execution_time = end_time - start_time
print("Execution time: {execution_time}")