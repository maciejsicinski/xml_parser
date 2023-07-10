import sqlglot
from sqlglot import exp

def addColumnAlias(sql_query):
    try:
        parsed = sqlglot.parse_one(sql_query)

        # Find all expressions in the SELECT statement
        expressions = parsed.find(exp.Select).args["expressions"]
        # Find the expression corresponding to the column name
        for i, expression in enumerate(expressions):
            if expression and not expression.alias and hasattr(expression, '__dict__'):
                new_expression = str(expression) + " " + "AS " + f"expression_{i+1}" 
                parsed_expression = sqlglot.parse_one(new_expression).find(exp.Expression)
                expressions[i] = parsed_expression
                #
                #print(parsed_expression)
        # Get the modified SQL query
        modified_query = str(parsed)

        return modified_query

    except Exception as e:
        print(f"Error occurred while parsing the SQL query: {e}")


# Example SQL query
sql_query = "SELECT col1, col2 + col3, col4 FROM table1 WHERE col5 > 10"

# Add alias for column "col2"
print("original query")
print(sql_query)
modified_query = addColumnAlias(sql_query)
print("modified query")
print(modified_query)