from datetime import datetime
from sqlalchemy import (MetaData, Table, Column, Integer, Numeric, String,
                        DateTime, ForeignKey, create_engine, insert, desc, select, func, cast,
                        and_, or_, not_, update)
metadata = MetaData()

cookies = Table('cookies', metadata,
                Column('cookie_id', Integer(), primary_key=True),
                Column('cookie_name', String(50), index=True),
                Column('cookie_recipe_url', String(255)),
                Column('cookie_sku', String(55)),
                Column('quantity', Integer()),
                Column('unit_cost', Numeric(12, 2))
                )
users = Table('users', metadata,
                Column('user_id', Integer(), primary_key=True),
                Column('customer_number', Integer(), autoincrement=True),
                Column('username', String(15), nullable=False, unique=True),
                Column('email_address', String(255), nullable=False),
                Column('phone', String(20), nullable=False),
                Column('password', String(25), nullable=False),
                Column('created_on', DateTime(), default=datetime.now),
                Column('updated_on', DateTime(), default=datetime.now, onupdate=datetime.now)
              )

orders = Table('orders', metadata,
                Column('order_id', Integer(), primary_key=True),
                Column('user_id', ForeignKey('users.user_id'))
               )

line_items = Table('line_items', metadata,
                    Column('line_items_id', Integer(), primary_key=True),
                    Column('order_id', ForeignKey('orders.order_id')),
                    Column('cookie_id', ForeignKey('cookies.cookie_id')),
                    Column('quantity', Integer()),
                    Column('extended_cost', Numeric(12, 2))
                   )

engine = create_engine('sqlite:///:memory:')
metadata.create_all(engine)
connection = engine.connect()

ins = cookies.insert().values(
    cookie_name="chocolate chip",
    cookie_recipe_url="http://some.aweso.me/cookie/recipe.html",
    cookie_sku="CC01",
    quantity="12",
    unit_cost="0.05"
)

result = connection.execute(ins)

ins = insert(cookies)   # or ins = cookies.insert()

result = connection.execute(ins,
    cookie_name="dark chocolate chip",
    cookie_recipe_url="http://some.aweso.me/cookie/recipe_dark.html",
    cookie_sku="CC02",
    quantity="1",
    unit_cost="0.75"
)

print(result.inserted_primary_key)

inventory_list = [
    {
        'cookie_name': 'peanut_butter',
        'cookie_recipe_url': 'http://some.aweso.me/cookie/peanut.html',
        'cookie_sku': 'PB01',
        'quantity': '24',
        'unit_cost': '0.25'
    },
    {
        'cookie_name': 'oatmeal raisin',
        'cookie_recipe_url': 'http://some.okay.me/cookie/raisin.html',
        'cookie_sku': 'EWW01',
        'quantity': '100',
        'unit_cost': '1.00'
    }
]

result = connection.execute(ins, inventory_list)

s = select([cookies])
rp = connection.execute(s)
# results = rp.fetchall()
# first_row = results[0]
# print(first_row[1])
# print(first_row.cookie_name)
# print(first_row[cookies.c.cookie_name])
for record in rp:
    print(record[cookies.c.cookie_name])

s = select([cookies.c.cookie_name, cookies.c.quantity])
s = s.order_by(cookies.c.quantity)

rp = connection.execute(s)
first = rp.first()
print(first)

s = select([cookies.c.cookie_name, cookies.c.quantity])
s = s.order_by(desc(cookies.c.quantity))
s = s.limit(2)
rp = connection.execute(s)
print(rp.fetchall())


s = select([func.sum(cookies.c.quantity)])
rp = connection.execute(s)
print(rp.scalar())

s = select([func.count(cookies.c.cookie_name)])
rp = connection.execute(s)
record = rp.first()
print(record.keys())
print(record.count_1)   # auto generate func_position func: count cookie_name: 1 => count_1

s = select([func.count(cookies.c.cookie_name).label('inventory_count')])
rp = connection.execute(s)
record = rp.first()
print(record.keys())
print(record.inventory_count)


###

s = select([cookies]).where(cookies.c.cookie_name == 'chocolate chip')
rp = connection.execute(s)
record = rp.first()
print(record.items())

s = select([cookies]).where(cookies.c.cookie_name.like('%chocolate%'))
rp = connection.execute(s)
for record in rp.fetchall():
    print(record.cookie_name)


s = select([cookies]).where(cookies.c.cookie_name.contains('dark'))
rp = connection.execute(s)
for record in rp.fetchall():
    print(record)

s = select([cookies.c.cookie_name, 'SKU-' + cookies.c.cookie_sku])

for row in connection.execute(s).fetchall():
    print(row)


s = select([cookies.c.cookie_name,
            cast((cookies.c.quantity * cookies.c.unit_cost),
                 Numeric(12, 2)).label('inv_cost')])

for row in connection.execute(s).fetchall():
    print('{} - {}'.format(row.cookie_name, row.inv_cost))

s = select([cookies]).where(
    and_(
        cookies.c.quantity > 23,
        cookies.c.unit_cost < 0.4
    )
)

for row in connection.execute(s):
    print(row.cookie_name)


customer_list = [
{
'username': 'cookiemon',
'email_address': 'mon@cookie.com',
'phone': '111-111-1111',
'password': 'password'
},
{
'username': 'cakeeater',
'email_address': 'cakeeater@cake.com',
'phone': '222-222-2222',
'password': 'password'
},
{
'username': 'pieguy',
'email_address': 'guy@pie.com',
'phone': '333-333-3333',
'password': 'password'
}
]
ins = users.insert()
result = connection.execute(ins, customer_list)
ins = insert(orders).values(user_id=1, order_id=1)
result = connection.execute(ins)
ins = insert(line_items)
order_items = [
{
'order_id': 1,
'cookie_id': 1,
'quantity': 2,
'extended_cost': 1.00
},
{
'order_id': 1,
'cookie_id': 3,
'quantity': 12,
'extended_cost': 3.00
}
]
result = connection.execute(ins, order_items)
ins = insert(orders).values(user_id=2, order_id=2)
result = connection.execute(ins)
ins = insert(line_items)
order_items = [{
'order_id': 2,
'cookie_id': 1,
'quantity': 24,
'extended_cost': 12.00
},
{
'order_id': 2,
'cookie_id': 4,
'quantity': 6,
'extended_cost': 6.00
}
]
result = connection.execute(ins, order_items)

columns = [orders.c.order_id, users.c.username, users.c.phone,
           cookies.c.cookie_name, line_items.c.quantity, line_items.c.extended_cost]

cookiemon_orders = select(columns)
cookiemon_orders = cookiemon_orders.select_from(orders.join(users).join(line_items).join(cookies))
cookiemon_orders = cookiemon_orders.where(users.c.username == 'cookiemon')
print(cookiemon_orders)
for row in connection.execute(cookiemon_orders):
    print(row)