import re
import sqlglot
from sqlglot import exp

def assignAliases(sql_query):
    # Pattern for matching expressions in the SELECT statement
    pattern = r"(?i)(?<=SELECT)\s+(.*?)(?=FROM)"

    # Find all expressions in the SELECT statement
    select_match = re.search(pattern, sql_query)
    if select_match:
        expressions = select_match.group(0).strip().split(',')

        # Assign aliases to expressions without existing aliases
        modified_query = re.sub(pattern, ', '.join(f"{expr} AS expr_{i+1}" if not re.search(r"\s+AS\s+", expr, re.I) else expr for i, expr in enumerate(expressions)), sql_query)

        return modified_query

    else:
        print("No SELECT statement found in the SQL query.")
        return sql_query
    
def assignAliasesSqlglot(sql_query):
    try:
        parsed = sqlglot.parse_one(sql_query)

        # Find all expressions in the SELECT statement
        expressions = parsed.find(exp.Select).args["expressions"]

        # Assign aliases to expressions without existing aliases
        for i, expression in enumerate(expressions):
            if not expression.alias:
                print("empty_expression") 
               # print(expression.alias)
                #expression.alias = f"expr_{i+1}" # CANT DO THAT

        # Get the modified SQL query
        modified_query = str(parsed)

        return modified_query

    except Exception as e:
        print(f"Error occurred while parsing the SQL query: {e}")

def find_columns(query):
    column_names = []
    try:
        for expression in sqlglot.parse_one(query).find(exp.Select).args["expressions"]:
            column_names.append(expression.alias_or_name)
    except Exception as e:
        return ["parse_error"]
    return column_names

# Example SQL query
sql_query = "SELECT col1, col2 + col3 FROM table1 WHERE col4 > 10"

# Assign aliases to expressions without existing aliases
modified_query = assignAliases(sql_query)
#modified_query_sqlglot = assignAliasesSqlglot(sql_query)

print(modified_query)

#print(modified_query_sqlglot)


