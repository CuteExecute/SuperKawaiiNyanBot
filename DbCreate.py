from sqlalchemy import create_engine, select, Table, Column, Integer, Boolean, String, MetaData, ForeignKey

DB_NAME = "KawaiiBot.db"
meta = MetaData()

# run this module if u wish created all database struct from scratch
# create tables
users = Table('Users', meta,
              Column('id_user', Integer, primary_key=True),
              Column('name', String(100), nullable=False),
              Column('discord_user_id', String(100), nullable=False)
              )

marks = Table('Marks', meta,
              Column('id_mark', Integer, primary_key=True),
              Column('user_id', Integer, ForeignKey("Users.id_user")),
              Column('date', String(100), nullable=False),
              Column('morning', Boolean, nullable=False),
              Column('day', Boolean, nullable=False),
              Column('evening', Boolean, nullable=False)
              )

print(users.c.name)  # print(users.columns.name)
print(users.primary_key)

print(marks.c.date)
# print(marks.c.primary_key)
print(marks.c)

# create database connection
engine = create_engine(f"sqlite:///{DB_NAME}", echo=True)
meta.create_all(engine)  # or users.create(engine), marks.create(engine)

conn = engine.connect()

# insert data in tables
ins_user_query_01 = users.insert().values(name='Lucius Annaeus Seneca', discord_user_id='dis1337322')
conn.execute(ins_user_query_01)
ins_user_query_02 = users.insert().values(name='Mr Adidas', discord_user_id='dis2281488')
conn.execute(ins_user_query_02)

ins_mark_query_01 = marks.insert().values(user_id=1, date='19-09-2022', morning=False, day=False, evening=True)
conn.execute(ins_mark_query_01)
ins_mark_query_02 = marks.insert().values(user_id=1, date='20-09-2022', morning=True, day=True, evening=False)
conn.execute(ins_mark_query_02)
ins_mark_query_03 = marks.insert().values(user_id=2, date='20-09-2022', morning=False, day=True, evening=False)
conn.execute(ins_mark_query_03)

sel_query = marks.select().where(marks.c.user_id == 1)
result = conn.execute(sel_query)

for row in result:
    print(row)
