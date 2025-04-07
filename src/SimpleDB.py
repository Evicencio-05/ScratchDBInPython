import os
import json
import re
import uuid
import threading
import src.parser as parser
import src.readwritelocks as ReadWriteLock
from threading import Lock

class SimpleDB:
    def __init__(self, db_file):
        self.db_file = db_file
        
        if not os.path.exists(db_file):
            with open(db_file, 'w') as f:
                json.dump({}, f)
                
        with open(db_file, 'r') as f:
            self.tables = json.load(f)
        
        self.tables = {}
        self.indexes = {}
        self.in_commit = False
        self.in_transaction = False
        self.locks = {}
        self.tx_lock = threading.Lock()
        self.thread_local = threading.local()
        self.metadata_lock = Lock()
        self.save_lock = Lock()
        self.row_locks = {}
        self.table_locks = {}
        
    @property
    def current_transaction_log(self):
        return getattr(self.thread_local, 'transaction_log', [])
        
    def begin_transaction(self):
        """
        Start a new transaction by clearing the transaction log.
        """
        if hasattr(self.thread_local, 'in_transaction') and self.thread_local.in_transaction:
            raise RuntimeError('Transaction already in progress in this thread.')
        
        self.thread_local.in_transaction = True
        self.thread_local.transaction_log = []
        
    def commit(self):
        """
        Apply all logged operations to the database and clear the log
        """
        if not hasattr(self.thread_local, 'in_transaction') or not self.thread_local.in_transaction:
            raise RuntimeError("No transaction in progress in this thread.")
        
        try:
            tables_involved = set(op["table"] for op in self.thread_local.transaction_log)
            locks = [self._get_table_lock(table) for table in sorted(tables_involved)]
            for lock in locks:
                lock.acquire_write()
            try:
                for op in self.thread_local.transaction_log:
                    table = op["table"]
                    if op["type"] == "insert":
                        self._commit_insert(table, op["row"])
                    elif op["type"] == "update":
                        self._commit_update(table, op["set_values"], op.get("where"))
                    elif op["type"] == "delete":
                        self._commit_delete(table, op.get("where"))
                        
                self.save()
            finally:
                for lock in locks:
                    lock.release_write()
        finally:
            self.thread_local.transaction_log = []
            self.thread_local.in_transaction = False
    
    def rollback(self):
        """
        Discard all operations in current transaction.
        """
        if not hasattr(self.thread_local, 'in_transaction') or not self.thread_local.in_transaction:
            raise RuntimeError("No transaction in progress in this thread.")
        
        self.thread_local.transaction_log = []
        self.thread_local.in_transaction = False

    def create_index(self, table_name, column):
        """
        Create an index for columns to make searching more efficient.
        """
        with self.metadata_lock:
            if table_name not in self.tables:
                raise ValueError("Table does not exist.")
            with self._get_lock(table_name):            
                self.indexes.setdefault(table_name, {})[column] = {}
                
                for i, row in enumerate(self.tables[table_name]["rows"]):
                    val = row[column]
                    self.indexes[table_name][column].setdefault(val, []).append(i)
        
    def save(self):
        """
        Save new data to json file.
        """
        with self.save_lock:
            with open(self.db_file, 'w') as f:
                json.dump(self.tables, f)
        
    def create_table(self, table_name: str, columns: list):
        """
        Create new a new table. Duh...
        """
        with self.metadata_lock:
            if table_name in self.tables:
                raise ValueError("Table already exists")
            
            self.tables[table_name] = {
                "columns": columns,
                "rows": []
            }
            self.save()
        
    def insert(self, table_name: str, rows: list):
        """
        Applies new rows to transaction log to wait for commit.
        """
        if hasattr(self.thread_local, 'in_transaction') and self.thread_local.in_transaction:
            self.thread_local.transaction_log.append({"type": "insert", "table": table_name, "row": rows})
        else:
            lock = self._get_table_lock(table_name)
            lock.acquire_write()
            try:
                self._commit_insert(table_name, rows)
                self.save()
            finally:
                lock.release_write()

    def _commit_insert(self, table_name: str, rows: list) -> None | ValueError | RuntimeError:
        """
        Inserts values into table from transaction log once committed.
        """
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        table = self.tables[table_name]
        
        if not isinstance(rows, list):
            raise RuntimeError("Rows are not of type list")
        else:
            for row in rows:
                if not set(row.keys()).issubset(set(table["columns"])) or len(row.keys()) == 0:
                    raise ValueError("Row does not match table schema")
                
                row = self._update_with_real_keys(row, table["columns"])
                row = self._set_row_id(row)
                row = self._align_row_to_schema(table_name, row)
                
                table["rows"].append(row)
    
    def select(self, table_name: str, columns: list, where=None):
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        lock = self._get_table_lock(table_name)
        lock.acquire_read()
        try:
            if where and table_name in self.indexes:
                for col, cond in where.items():
                    if col in self.indexes[table_name] and "eq" in cond:
                        indices = self.indexes[table_name][col].get(cond["eq"], [])
                        rows = [self.tables[table_name]["rows"][i] for i in indices]
                        if columns == ["*"]:
                            return rows
                        else:
                            return [{c: row[c] for c in columns} for row in rows]
            else:
                table = self.tables[table_name]
                rows = table["rows"]
                
                if where:
                    rows = [row for row in rows if self._apply_where(row, where)]
                if columns == ["*"]:
                    return rows
                else:
                    return [{col: row[col] for col in columns} for row in rows]
        finally:
            lock.release_read()

    def update(self, table_name, set_values, where=None) -> None:
        """
        Updates the table with new values.
        """
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        if hasattr(self.thread_local, 'in_transaction') and self.thread_local.in_transaction:
            self.thread_local.transaction_log.append({"type": "update", "table": table_name, "set_values": set_values, "where": where})
        else:
            lock = self._get_table_lock(table_name)
            lock.acquire_write()
            try:
                self._commit_update(table_name, set_values, where)
                self.save()
            finally:
                lock.release_write()
                

    def _commit_update(self, table_name, set_values, where=None):
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        table = self.tables[table_name]
        
        for i, row in enumerate(table["rows"]):
            if not where or self._apply_where(row, where):
                table["rows"][i].update(set_values)
                    
    def delete(self, table_name, where=None) -> None | ValueError | TypeError:
        """
        Deletes row(s) from the table.
        """
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        if hasattr(self.thread_local, 'in_transaction') and self.thread_local.in_transaction:
            self.thread_local.transaction_log.append({"type": "delete", "table": table_name, "where": where})
        else:
            lock = self._get_table_lock(table_name)
            lock.acquire_write()
            try:
                self._commit_delete(table_name, where)
                self.save()
            finally:
                lock.release_write()
        
    def _commit_delete(self, table_name, where=None):
        if table_name not in self.tables:
            raise ValueError("Table does not exist")
        
        table = self.tables[table_name]
        
        if not where:
            table["rows"] = []
        else:
            table["rows"] = [row for row in table["rows"] if not self._apply_where(row, where)]    
    
    def execute(self, query_str):
        """
        Parses the string and continues to appropriate function.
        """
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
    
    def _get_lock(self, table_name):
        if table_name not in self.locks:
            self.locks[table_name] = Lock()
        return self.locks[table_name]
    
    def _get_row_lock(self, table_name, row_id):
        if table_name not in self.row_locks:
            self.row_locks[table_name] = {}
        if row_id not in self.row_locks[table_name]:
            self.row_locks[table_name][row_id] = Lock()
            return self.row_locks[table_name][row_id]
        
    def _get_table_lock(self, table_name):
        if table_name not in self.table_locks:
            self.table_locks[table_name] = ReadWriteLock.ReadWriteLock()
        return self.table_locks[table_name]
        
    def _set_row_id(self, row: dict) -> dict:
        """
        Set row id for rows. Not included in parsed values.
        
        * Need to make immutable.
        * Might make optional.
        """
        new_id: dict = {"id": str(uuid.uuid4())}
        new_id.update(row)
        return new_id

    def _align_row_to_schema(self, table_name: str, row: dict) -> dict:
        """
        Fixes the rows to fit with schema.
        """
        result = {}
        for key in self.tables[table_name]["columns"]:
            result[key] = row.get(key, None)
        return result
    
    def _apply_where(self, row, where) -> bool | TypeError:
        """
        Checks if value (if comparable) is greater than ('gt'), less than ('lt'), or equal ('eq') to parsed value.
        """
        for col, condition in where.items():
            for op, value in condition.items():
                if not type(row[col]) == type(value):
                    raise TypeError("Row value and compare value are not of the same type")
                elif op == "eq" and row[col] != value:
                    return False
                elif op == "gt" and row[col] <= value:
                    return False
                elif op == "lt" and row[col] >= value:
                    return False
        return True
    
    def _update_with_real_keys(self, temp_dict: dict, key_map: list) -> dict:
        """
        Updates an entry with all keys from the parent table.
        """
        result = {}
        
        pattern = r'^temp_\d+$'
        for key, value in temp_dict.items():
            if bool(re.match(pattern, key)):
                index = int(key.split('_')[1])
                
                if index < len(key_map):
                    result[key_map[index]] = value
                else:
                    result[key] = value
            else:
                result[key] = value
        
        return result