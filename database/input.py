import SimpleDB as sdb

def main():
    query: str = "INSERT INTO users (id, name, age, word) VALUES (1, 'Test User1', 111, 'TEST 1'), (2, 'Test User2', 222, 'TEST 2')"
    
    db = sdb.SimpleDB("db.json")
    # db.insert("users", [{"id": 1, "name": "Alice", "age": 30, "word": "NULL"},
    #                     {"id": 2, "name": "Bob", "age": 25, "word": "NULL"},
    #                     {"id": 3, "name": "Charlie", "age": 35, "word": "NULL"}])
    # db.commit()
    print(db.execute(query))
    db.commit()
    
    
if __name__ == "__main__":
    main()