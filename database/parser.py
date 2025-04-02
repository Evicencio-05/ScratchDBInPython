import re

def get_where(parts: list[any], query: dict) -> None:
    if "WHERE" in parts:
            where_idx = parts.index('WHERE')
            col, op, val = parts[where_idx + 1], parts[where_idx + 2], parts[where_idx + 3]
            val = int(val) if val.isdigit() else val
            
            if isinstance(val, str):
                if val.startswith("'") and val.endswith("'"):
                    val = val.strip("'")
            
            op_map = {
                '>': "gt",
                '=': "eq",
                '<': "lt"
                }
            query["where"][col] = {op_map[op]: val}

def parse_query(query_str):
    pattern = r"'[^']*'|\"[^\"]*\"|\w+|[^\w\s,]"
    parts = re.findall(pattern, query_str)
    query = {}
    
    if parts[0].upper() == 'SELECT':
        query["type"] = "SELECT"
        query["where"] = {}
        cols_end = parts.index('FROM')
        query["columns"] = parts[1:cols_end][0].split(",")
        columns_str = query_str.split('SELECT')[1].split('FROM')[0].strip()
        query["columns"] = [col.strip() for col in columns_str.split(',')]
        query["table"] = parts[cols_end + 1]
        get_where(parts, query)
        
    elif parts[0].upper() == "INSERT" and parts[1].upper() == "INTO":
        
        # FIX: Doesn't account for missing values
        
        query["type"] = "INSERT INTO"
        table_name: str = parts[parts.index("INTO") + 1]
        query["table"] = table_name
        
        pattern = r'\((.*?)\)'
        input_keys = re.findall(pattern, query_str.split(table_name)[1].split("VALUES")[0].strip())
        input_values = re.findall(pattern, query_str.split("VALUES")[1].strip())
        
        lists = []
        for input in input_keys:
            items = [item.strip() for item in input.split(',')]
            lists.append(items)
        for input in input_values:
            items = [item.strip() for item in input.split(',')]
            lists.append(items)
        keys = lists[0]
        values = lists[1:len(lists)]
        values = [[int(val) if val.isdigit() else val for val in value_list] for value_list in values]
        
        rows: list = []
        for value_list in values:
            for i in range(len(value_list)):
                if isinstance(value_list[i], str):
                    if value_list[i].startswith('\'') and value_list[i].endswith('\''):
                        value_list[i] = value_list[i].strip('\'')
            rows.append(dict(zip(keys, value_list)))
        
        query["values"] = rows
        
    elif parts[0].upper() == "DELETE":
        query["type"] = "DELETE"
        query["where"] = {}
        
        table_name: str = parts[parts.index("FROM") + 1]
        query["table"] = table_name
    
        get_where(parts, query)
        
    elif parts[0].upper() == "UPDATE":
        query["type"] = "UPDATE"
        query["where"] = {}
        
        table_name: str = parts[parts.index("UPDATE") + 1]
        query["table"] = table_name
        
        get_where(parts, query)
        
        updated_values_parts = query_str.split('SET')[1].split("WHERE")[0].strip()
        updated_values_parts = updated_values_parts.split(',')
        
        values: dict = {}
        for part in updated_values_parts:
            key, value = part.split('=')
            key = key.strip()
            value = value.strip()
            
            value = int(value) if value.isdigit() else value
            
            if isinstance(value, str):
                if value.startswith("'") and value.endswith("'"):
                    value = value.strip("'")

                
            values[key] = value
            
        query["values"] = values
            
    return query