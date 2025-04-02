import json
import pytest
import database.SimpleDB as sdb

class TestSimpleDB:
    
    @pytest.fixture
    def test_create_file(self, tmp_path):
        dir = tmp_path / "my_temp_dir"
        dir.mkdir()
        
        temp_file = dir / "test_file.json"
        with open(temp_file, 'w') as f:
            json.dump({}, f)
            
        with open(temp_file, 'r') as f:
            data = json.load(f)
            
        assert temp_file.is_file()
        assert data == {}
    
    @pytest.fixture
    def db(self, tmp_path):
        dir = tmp_path / "my_temp_dir"
        dir.mkdir()
        
        temp_file = dir / "test_file.json"
        db = sdb.SimpleDB(temp_file)
        
        return db
        
    @pytest.fixture
    def populated_db(self, tmp_path):
        dir = tmp_path / "my_temp_dir"
        dir.mkdir()
        
        temp_file = dir / "test_file.json"
        populated_db = sdb.SimpleDB(temp_file)
        
        populated_db.create_table("test_table", ["id", "name", "age"])
        populated_db.insert("test_table", [{"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Charlie", "age": 35}])
        populated_db.commit()
        
        return populated_db
    
    def test_create_table(self, db):
        db.create_table("users", ["id", "name", "age"])
        
        assert "users" in db.tables
        assert db.tables["users"]["columns"] == ["id", "name", "age"]
        assert db.tables["users"]["rows"] == []
        assert db.transaction_log == []
        assert db.in_commit == False
        assert db.locks == {}
        assert db.indexes == {}
        
    def test_insert(self, db):
        db.create_table("users", ["id", "name", "age"])
        db.insert("users", [{"id": 1, "name": "Test User", "age": 20}])
        
        assert db.transaction_log == [{"type": "insert",
            "table": "users",
            "row": [{"id": 1, "name": "Test User", "age": 20}]}]
        
        db.commit()
        
        assert db.transaction_log == []
        assert len(db.tables["users"]["rows"]) == 1
        assert db.tables["users"]["rows"][0] == {'id': 1, 'name': 'Test User', 'age': 20}
        
        with pytest.raises(ValueError) as e_info:
            db._commit_insert("Non_existent_table", [{}])
        assert str(e_info.value) == "Table does not exist"
        
        with pytest.raises(RuntimeError) as e_info:
            db._commit_insert("users", {})
        assert str(e_info.value) == "Rows are not of type list"
        
        with pytest.raises(ValueError) as e_info:
            db._commit_insert("users", [{}])
        assert str(e_info.value) == "Row does not match table schema"
    
    def test_select(self, populated_db):
        db = populated_db
        
        assert db.select("test_table", ['*'], {'id': {'eq': 1}}) == [{'id': 1, 'name': 'Alice', 'age': 30}]
        
    def test_apply_where(self, populated_db):
        db = populated_db
        
        assert db._apply_where(db.tables["test_table"]["rows"][0], {"id": {"eq": 1}}) == True
        assert db._apply_where(db.tables["test_table"]["rows"][0], {"age": {"gt": -5}}) == True
        assert db._apply_where(db.tables["test_table"]["rows"][0], {"age": {"eq": 25}}) == False
        assert db._apply_where(db.tables["test_table"]["rows"][0], {"age": {"gt": 30}}) == False
        db._apply_where(db.tables["test_table"]["rows"][0], {"name": {"eq": "Alice"}})
        
        with pytest.raises(TypeError) as e_info:
            db._apply_where(db.tables["test_table"]["rows"][0], {"age": {"eq": '30'}})
        assert str(e_info.value) == "Row value and compare value are not of the same type"
        
    def test_update(self, populated_db):
        db = populated_db
        
        db.update("test_table", {"name": "John", "age": 20}, {"id": {"eq": 1}})
        
        assert db.tables["test_table"]["rows"][0] == {'id': 1, 'name': 'John', 'age': 20}
        assert db.tables["test_table"]["rows"][1] == {"id": 2, "name": "Bob", "age": 25}
        
        db.update("test_table", {"name": "TEST", "age": 0}, None)
        assert db.tables["test_table"]["rows"][0] == {"id": 1, "name": "TEST", "age": 0}
        assert db.tables["test_table"]["rows"][1] == {"id": 2, "name": "TEST", "age": 0}
        assert db.tables["test_table"]["rows"][2] == {"id": 3, "name": "TEST", "age": 0}
        
        with pytest.raises(ValueError) as e_info:
            db.update("Non_existent_table", {"name": "TEST", "age": 0}, None)
        assert str(e_info.value) == "Table does not exist"
            