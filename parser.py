def parse_query(query_str):
    parts = query_str.split()
    query = {"where": {}}
    
    if parts[0].upper == "SELECT":
        query["type"] = "SELECT"
        cols_end = parts.index("FROM")
        query["columns"] = parts[1:cols_end][0].split(',')
        query["table"] = parts[cols_end + 1]
        if "WHERE" in parts:
            where_idx = parts.index("WHERE")
            col, op, val = parts[where_idx + 1], parts[where_idx + 2], parts[where_idx + 3]
            op_map = {
                '>': "gt",
                '=': "eq",
                '<': "lt"
                }
            query["where"][col] = {op_map[op]: val}
            
    return query