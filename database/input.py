import SimpleDB as sdb

def main():
    query: str = "INSERT INTO users (id, name, age, word) VALUES (1, Test User, 111, TEST TEST)"
    
    db = sdb.SimpleDB("db.json")
    print(db.execute(query))
    
    
if __name__ == "__main__":
    main()