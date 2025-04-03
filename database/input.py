import SimpleDB as sdb

def main():
    # query: str = "UPDATE users SET name = 'Bob', age = 10, word = 'Test2' WHERE id = 1"
    query: str = "INSERT INTO users VALUES (1, 'first', 0001, 'temp'), (2, 'second', 0010, 'temp') "
    
    db = sdb.SimpleDB("db.json")
    # db.insert("users", [{"id": 1, "name": "Alice", "age": 30, "word": "NULL"},
    #                     {"id": 2, "name": "Bob", "age": 25, "word": "NULL"},
    #                     {"id": 3, "name": "Charlie", "age": 35, "word": "NULL"}])
    # db.commit()
    print(db.execute(query))
    db.commit()
    
    
if __name__ == "__main__":
    main()