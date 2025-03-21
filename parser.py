import re

def parse_query(query_str):
    parts = re.findall(r'\w+|[^\w\s,]', query_str)
    query = {"where": {}}
    
    if parts[0].upper() == 'SELECT':
        query["type"] = "SELECT"
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
            
    return query