import re

def assignAliases(sql_query):
    # Pattern for matching expressions in the SELECT statement
    pattern = r"(?i)(?<=SELECT)\s+(.*?)(?=FROM)"

    # Find all expressions in the SELECT statement
    select_match = re.search(pattern, sql_query)
    if select_match:
        expressions = select_match.group(0).strip().split(',')

        # Assign aliases to expressions without existing aliases
        modified_expressions = []
        for i, expr in enumerate(expressions):
            expr = expr.strip()
            if not re.search(r"\s+AS\s+", expr, re.I):
                modified_expressions.append(f"{expr} AS expr_{i+1}")
            else:
                modified_expressions.append(expr)

        # Update the SQL query with modified expressions and add spaces
        modified_query = re.sub(pattern, ', '.join(modified_expressions), sql_query)
        modified_query = modified_query.replace("SELECT", "SELECT ").replace("FROM", " FROM")

        return modified_query

    else:
        print("No SELECT statement found in the SQL query.")
        return sql_query

# Example SQL query
sql_query = "SELECT col1, col2 + col3 FROM table1 WHERE col4 > 10"

# Assign aliases to expressions without existing aliases
modified_query = assignAliases(sql_query)

print(modified_query)
