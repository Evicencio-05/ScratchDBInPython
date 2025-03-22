import SimpleDB as sdb

def main():
    query: str = input("Please enter your query. ")
    
    db = sdb.SimpleDB("db.json")
    print(db.execute(query))
    
    
if __name__ == "__main__":
    main()