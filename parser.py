import re

def parse_query(query_str):
    parts = re.findall(r'\w+|[^\w\s,]', query_str)
    query = {}
    
    if parts[0].upper() == 'SELECT':
        query["type"] = "SELECT"
        query["where"] = {}
        cols_end = parts.index('FROM')
        columns_str = query_str.split('SELECT')[1].split('FROM')[0].strip()
        query["columns"] = [col.strip() for col in columns_str.split(',')]
        query["table"] = parts[cols_end + 1]
        if "WHERE" in parts:
            where_idx = parts.index('WHERE')
            col, op, val = parts[where_idx + 1], parts[where_idx + 2], int(parts[where_idx + 3])
            op_map = {
                '>': "gt",
                '=': "eq",
                '<': "lt"
                }
            query["where"][col] = {op_map[op]: val}
    elif parts[0].upper() == "INSERT" and parts[1].upper() == "INTO":
        query["type"] = "INSERT INTO"
        
        table_name: str = parts[parts.index("INTO") + 1]
        query["table"] = table_name
        
        input_values = re.findall(r'\((.*?)\)', query_str)
        
        lists = []
        for input in input_values:
            items = [item.strip() for item in input.split(',')]
            lists.append(items)
        keys = lists[0]
        values = lists[1]
        values = [int(val) if val.isdigit() else val for val in values]
        row = dict(zip(keys, values))
        query["values"] = row
    elif parts[0].upper() == "DELETE":
        query["type"] = "DELETE"
        query["where"] = {}
        
        table_name: str = parts[parts.index("FROM") + 1]
        query["table"] = table_name
    
        if "WHERE" in parts:
            where_idx = parts.index('WHERE')
            col, op, val = parts[where_idx + 1], parts[where_idx + 2], int(parts[where_idx + 3])
            op_map = {
                '>': "gt",
                '=': "eq",
                '<': "lt"
                }
            query["where"][col] = {op_map[op]: val}    
        else:
            query["where"] = None
            
    return query