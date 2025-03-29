import SimpleDB
# from time import perf_counter_ns

db = SimpleDB.SimpleDB("db.json")

# db.create_table("users", ["id", "name", "age", "word"])

# db.insert("users", {"id": "5", "name": "Billl", "age": 14, "word": "test"})
# db.update("users", {"age": 4, "word": "test"}, where = {"age": {"gt": 25}})
# db.execute("SELECT name, age FROM users WHERE age > 25")
# db.execute("SELECT * FROM users WHERE word = 'TEST'")

# print(db.execute("INSERT INTO users (id, name, age) VALUES (3, Charlie, 22)"))
# print(db.execute("DELETE FROM users"))
# print(db.execute("UPDATE users SET word = 'TEST'"))

# db.create_index('users', 'word')

# start_time = perf_counter_ns()
# end_time = perf_counter_ns()
# execution_time = end_time - start_time
# print(f"Execution time: {execution_time}")

db.being_transaction()
db.insert("users", {"id": 101, "name": "John Doe", "age": 14, "word": "TRANSACTION_TEST"})
db.rollback()
print(db.select("users", ["*"]))