import json
import parser

class SimpleDB:
    def __init__(self, db_file):
        self.db_file = db_file
        
        try:
            with open(db_file, 'r') as f:
                self.tables = json.load(f)
        except FileNotFoundError:
            self.tables = {}
            
        self.indexes = {}
        
    def create_index(self, table_name, column):
        if table_name not in self.tables:
            raise ValueError("Table does not exist.")
        
        self.indexes.setdefault(table_name, {})[column] = {}
        
        for i, row in enumerate(self.tables[table_name]["rows"]):
            val = row[column]
            self.indexes[table_name][column].setdefault(val, []).append(i)
        
    def save(self):
        with open(self.db_file, 'w') as f:
            json.dump(self.tables, f)
        
    def create_table(self, table_name, columns):
        if table_name in self.tables:
            raise ValueError("Table already exists")
        
        self.tables[table_name] = {
            "columns": columns,
            "rows": []
        }
        self.save()
        
    def insert(self, table_name, row):
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        table = self.tables[table_name]
        
        if set(row.keys()) != set(table["columns"]):
            raise ValueError("Row does not match table schema")
        
        table["rows"].append(row)
        self.save()
    
    def select(self, table_name, columns, where=None):
        if where and table_name in self.indexes:
            for col, cond in where.items():
                if col in self.indexes[table_name] and "eq" in cond:
                    indices = self.indexes[table_name][col].get(cond["eq"], [])
                    rows = [self.tables[table_name]["rows"][i] for i in indices]
                    return [{c: row[c] for c in columns} for row in rows]
        
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        table = self.tables[table_name]
        rows = table["rows"]
        
        if where:
            rows = [row for row in rows if self._apply_where(row, where)]
        if columns == "*":
            return rows
            
        return [{col: row[col] for col in columns} for row in rows]

    def _apply_where(self, row, where) -> bool:
        for col, condition in where.items():
            for op, value in condition.items():
                if op == "eq" and row[col] != value:
                    return False
                elif op == "gt" and row[col] <= value:
                    return False
                elif op == "lt" and row[col] >= value:
                    return False
        return True
    
    def update(self, table_name, set_values, where=None) -> None:
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        table = self.tables[table_name]
        rows = table["rows"]
        
        for i, row in enumerate(rows):
            if not where or self._apply_where(row, where):
                rows[i].update(set_values)
        self.save()
        
    def delete(self, table_name, where=None) -> None:
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        table = self.tables[table_name]
        
        if not where:
            table["rows"] = []
        else:
            table["rows"] = [row for row in table["rows"] if not 
                            self._apply_where(row, where)]
        self.save()
        
    def execute(self, query_str):
        query: dict = parser.parse_query(query_str)
        if query["type"] == "SELECT":
            return self.select(query["table"],
                                query["columns"] , query.get("where"))
        elif query["type"] == "INSERT INTO":
            return self.insert(query["table"], query["values"])
        elif query["type"] == "DELETE":
            return self.delete(query["table"], query.get("where"))
        elif query["type"] == "UPDATE":
            return self.update(query["table"], query["values"], query.get("where"))
        return None
    