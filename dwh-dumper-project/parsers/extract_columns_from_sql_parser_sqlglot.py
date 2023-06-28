import sqlglot
import sqlglot.expressions as exp
from sqlglot import parse_one, exp

query = """
with cte as(select 1 as wow from table)
select
    sales.order_id as id,
    p.product_name,
    sum(p.price) as sales_volume,
    sum(p.price),
    2 a,
    3 as aaaaaaaa
from sales
right join products as p
    on sales.product_id=p.product_id
    join cte on 1=1
group by id, p.product_name;
"""

column_names = []

for expression in sqlglot.parse_one(query).find(exp.Select).args["expressions"]:
        column_names.append(expression.alias_or_name)
print(column_names)