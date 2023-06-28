import sqlparse
def find_selected_columns(query) -> list[str]:
    tokens = sqlparse.parse(query)[0].tokens
    found_select = False
    for token in tokens:
        if found_select:
            if isinstance(token, sqlparse.sql.IdentifierList):
                return [
                    col.value.split(" ")[-1].strip("`").rpartition('.')[-1]
                    for col in token.tokens
                    if isinstance(col, sqlparse.sql.Identifier)
                ]
        else:
            found_select = token.match(sqlparse.tokens.Keyword.DML, ["select", "SELECT"])
    raise Exception("Could not find a select statement. Weired query :)")


query = """
with cte as(select 1 as wow from table)
select
    sales.order_id as id,
    p.product_name,
    sum(p.price) as sales_volume,
    sum(p.price),
    2 a,
    cte.*
from sales
right join products as p
    on sales.product_id=p.product_id
    join cte on 1=1
group by id, p.product_name;

"""

print(find_selected_columns(query))