import os
import json
import pytest
from SimpleDB import SimpleDB
import tempfile

class TestSimpleDB:
    @pytest.fixture
    def temp_db_file(self):
        fd, path = tempfile.mkstemp()
        os.close(fd)
        yield path
        os.unlink(path)
        
    @pytest.fixture
    def db(self, temp_db_file):
        db = SimpleDB(temp_db_file)
        return db
    
    @pytest.fixture
    def populated_db(self, temp_db_file):
        with open(temp_db_file, 'w') as f:
            json.dump({
                "test_table": {
                    "columns": ["id", "name", "age"],
                    "rows": [
                        {"id": 1, "name": "Alice", "age": 30},
                        {"id": 2, "name": "Bob", "age": 25},
                        {"id": 3, "name": "Charlie", "age": 35}
                    ]
                }
            }, f)
        return SimpleDB(temp_db_file)
    
    def test_create_table(self, db):
        db.create_table("users", ["id", "name", "age"])
        assert "users" in db.tables
        assert db.tables["users"]["columns"] == ["id", "name", "age"]
        assert db.tables["users"]["rows"] == []
        
    def test_insert(self, db):
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", {"id": 1, "name": "Test User", "age": 30})
        db.commit()
        
        assert len(db.tables["users"]["rows"]) == 1
        assert db.tables["users"]["rows"][0] == {"id": 1, "name": "Test User", "age": 30}
        
    def test_select_all(self, populated_db):
        results = populated_db.select("test_table", ["*"])
        assert len(results) == 3
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Bob"
        assert results[2]["name"] == "Charlie"
        
    def test_select_columns(self, populated_db):
        results = populated_db.select("test_table", ["name", "age"])
        assert len(results) == 3
        assert "id" not in results[0]
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30
        
    def test_select_with_where(self, populated_db):
        results = populated_db.select("test_table", ["*"], {"age": {"gt": 25}})
        assert len(results) == 2
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Charlie"
    
    def test_update(self, populated_db):
        populated_db.update("test_table", {"age": 40}, {"name": {"eq": "Alice"}})
        results = populated_db.select("test_table", ["*"], {"name": {"eq": "Alice"}})
        assert results[0]["age"] == 40
    
    def test_delete(self, populated_db):
        populated_db.delete("test_table", {"name": {"eq": "Bob"}})
        results = populated_db.select("test_table", ["*"])
        assert len(results) == 2
        assert all(row["name"] != "Bob" for row in results)
    
    def test_transaction_commit(self, db):
        db.create_table("users", ["id", "name", "age"])
        
        db.begin_transaction()
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        db.commit()
        
        results = db.select("users", ["*"])
        assert len(results) == 2
    
    def test_transaction_rollback(self, db):
        db.create_table("users", ["id", "name", "age"])
        
        db.insert("users", {"id": 1, "name": "Alice", "age": 30})
        db.commit()
        
        db.begin_transaction()
        db.insert("users", {"id": 2, "name": "Bob", "age": 25})
        db.rollback()
        
        results = db.select("users", ["*"])
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
    
    def test_index(self, populated_db):
        populated_db.create_index("test_table", "age")
        
        assert "test_table" in populated_db.indexes
        assert "age" in populated_db.indexes["test_table"]
        
        results = populated_db.select("test_table", ["*"], {"age": {"eq": 30}})
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
    
    def test_execute_select(self, populated_db):
        results = populated_db.execute("SELECT * FROM test_table WHERE age = 30")
        assert len(results) == 1
        assert results[0]["name"] == "Alice"