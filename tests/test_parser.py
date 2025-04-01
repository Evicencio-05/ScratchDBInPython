import pytest
from database.parser import parse_query

class TestParser:
    def test_parse_select_query(self):
        query = "SELECT id,name,age FROM users WHERE age > 30"
        parsed = parse_query(query)
        
        assert parsed["type"] == "SELECT"
        assert parsed["columns"] == ["id", "name", "age"]
        assert parsed["table"] == "users"
        assert parsed["where"] == {"age": {"gt": 30}}
    
    def test_parse_insert_query(self):
        query = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
        parsed = parse_query(query)
        
        assert parsed["type"] == "INSERT INTO"
        assert parsed["table"] == "users"
        assert parsed["values"] == {"id": 1, "name": "Alice", "age": 30}
    
    def test_parse_update_query(self):
        query = "UPDATE users SET name='Bob', age=25 WHERE id = 1"
        parsed = parse_query(query)
        
        assert parsed["type"] == "UPDATE"
        assert parsed["table"] == "users"
        assert parsed["values"] == {"name": "Bob", "age": 25}
        assert parsed["where"] == {"id": {"eq": 1}}
    
    def test_parse_delete_query(self):
        query = "DELETE FROM users WHERE id = 1"
        parsed = parse_query(query)
        
        assert parsed["type"] == "DELETE"
        assert parsed["table"] == "users"
        assert parsed["where"] == {"id": {"eq": 1}}