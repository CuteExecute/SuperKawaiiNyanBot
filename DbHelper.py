from sqlalchemy import create_engine, select, Table, Column, Integer, Boolean, String, MetaData, ForeignKey, func, event
from sqlalchemy.sql import select, and_, extract
import datetime
from datetime import datetime

DB_NAME = "KawaiiBot.db"

engine = create_engine(f"sqlite:///{DB_NAME}", echo=False)
meta = MetaData(engine)

# fix new version - Table('mytable', metadata, autoload_with=engine, extend_existing=True)
users = Table('Users', meta, autoload=True)
# old version - Table('Marks', meta, autoload=True)
marks = Table('Marks', meta, autoload=True)

connection = engine.connect()


# other func
def get_month_by_date_string(date_string: str):
    return datetime.strptime(date_string, '%Y-%m-%d').month


def get_username_by_discord_id(dis_id) -> str:
    select_name_query = select(users.c.name).where(users.c.discord_user_id == dis_id)
    select_name = connection.execute(select_name_query)
    result = select_name.fetchall()
    return result[0][0]


# Users table CRUD and other
def is_user_exists(dis_id):
    is_user_exists_query = select(users).where(users.c.discord_user_id == dis_id)
    result = connection.execute(is_user_exists_query).fetchall()
    return bool(result)


def get_user_id_by_discord_id(dis_id) -> int:
    id_user_query = select(users.c.id_user).where(users.c.discord_user_id == dis_id)
    res = connection.execute(id_user_query).fetchall()
    return int(res[0][0])


def user_create(name, dis_id):
    if not is_user_exists(dis_id):
        insert_user_query = users.insert().values(name=f'{name}', discord_user_id=f'{dis_id}')
        connection.execute(insert_user_query)
    else:
        print("Такой уже есть!")


def user_update(dis_id, new_name):
    update_user_query = users.update().where(users.c.discord_user_id == dis_id).values(name=f'{new_name}')
    connection.execute(update_user_query)


def user_delete(dis_id):
    delete_user_query = users.delete().where(users.c.discord_user_id == f'{dis_id}')
    connection.execute(delete_user_query)


def user_clear_all_marks(dis_id):
    user_all_marks_reset_query = marks.delete().where(marks.c.user_id == get_user_id_by_discord_id(dis_id))
    connection.execute(user_all_marks_reset_query)


# Marks CRUD
def is_date_exists_for_user(date, dis_id):
    is_date_exists_query = select(marks).where(marks.c.date == date) \
        .where(marks.c.user_id == get_user_id_by_discord_id(dis_id))
    result = connection.execute(is_date_exists_query)
    return bool(result.fetchall())


def mark_set_for_user_by_date(name: str, dis_id: str, event_time: str, date: str):
    current_date = date

    if not is_user_exists(dis_id):
        user_create(name, dis_id)
    else:
        print('THIS USER IS ALREADY EXIST')

    if not is_date_exists_for_user(current_date, dis_id):

        mark_to_set = {
            'mrg': False,
            'day': False,
            'evg': False
        }

        if event_time == 'mrg':
            mark_to_set['mrg'] = True
        if event_time == 'day':
            mark_to_set['day'] = True
        if event_time == 'evg':
            mark_to_set['evg'] = True

        create_mark_query = marks.insert().values(user_id=get_user_id_by_discord_id(dis_id),
                                                  date=f"{current_date}",
                                                  morning=mark_to_set['mrg'],
                                                  day=mark_to_set['day'],
                                                  evening=mark_to_set['evg'])
        connection.execute(create_mark_query)
    else:
        if not is_user_exists(dis_id):
            user_create(name, dis_id)

        if event_time == 'mrg':
            create_mark_query = marks.update() \
                .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
                .where(marks.c.date == current_date) \
                .values(morning=True)
            connection.execute(create_mark_query)

        if event_time == 'day':
            create_mark_query = marks.update() \
                .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
                .where(marks.c.date == current_date) \
                .values(day=True)
            connection.execute(create_mark_query)

        if event_time == 'evg':
            create_mark_query = marks.update() \
                .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
                .where(marks.c.date == current_date) \
                .values(evening=True)
            connection.execute(create_mark_query)


def mark_remove_for_user_by_date(dis_id, event_time, date):
    if event_time == 'mrg':
        mark_remove_for_user_by_date_query = marks.update() \
            .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
            .where(marks.c.date == date) \
            .values(morning=False)
        connection.execute(mark_remove_for_user_by_date_query)

    if event_time == 'day':
        mark_remove_for_user_by_date_query = marks.update() \
            .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
            .where(marks.c.date == date) \
            .values(day=False)
        connection.execute(mark_remove_for_user_by_date_query)

    if event_time == 'evg':
        mark_remove_for_user_by_date_query = marks.update() \
            .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
            .where(marks.c.date == date) \
            .values(evening=False)
        connection.execute(mark_remove_for_user_by_date_query)

    delete_empty_string_for_user(dis_id)


# other func for db
def clear_all_table():
    for table in reversed(meta.sorted_tables):
        connection.execute(table.delete())


# SELECT queries
def select_marks_by_user_per_month(dis_id) -> []:
    select_query = select(users.c.name, marks.c.date, marks.c.morning, marks.c.day, marks.c.evening).join(marks) \
        .where(marks.c.user_id == users.c.id_user) \
        .where(users.c.id_user == get_user_id_by_discord_id(dis_id)) \
        .filter(extract('month', marks.c.date) == datetime.now().month)

    result = connection.execute(select_query)
    return result


def select_marks_by_user_all_date(dis_id) -> []:
    select_query = select(users.c.name, marks.c.date, marks.c.morning, marks.c.day, marks.c.evening).join(marks) \
        .where(marks.c.user_id == users.c.id_user) \
        .where(users.c.id_user == get_user_id_by_discord_id(dis_id))

    result = connection.execute(select_query)
    return result


def select_all_marks_count_per_month_for_user(dis_id) -> []:
    mrg_count = select([func.count()]).select_from(marks) \
        .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
        .where(marks.c.morning == True) \
        .filter(extract('month', marks.c.date) == datetime.now().month).scalar()
    day_count = select([func.count()]).select_from(marks) \
        .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
        .where(marks.c.day == True) \
        .filter(extract('month', marks.c.date) == datetime.now().month).scalar()
    evg_count = select([func.count()]).select_from(marks) \
        .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
        .where(marks.c.evening == True) \
        .filter(extract('month', marks.c.date) == datetime.now().month).scalar()
    all_count = mrg_count + day_count + evg_count

    return [mrg_count, day_count, evg_count, all_count]


def select_all_marks_count_all_time_for_user(dis_id) -> []:
    mrg_count = select([func.count()]).select_from(marks) \
        .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
        .where(marks.c.morning == True).scalar()
    day_count = select([func.count()]).select_from(marks) \
        .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
        .where(marks.c.day == True).scalar()
    evg_count = select([func.count()]).select_from(marks) \
        .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
        .where(marks.c.evening == True).scalar()
    all_count = mrg_count + day_count + evg_count

    return [mrg_count, day_count, evg_count, all_count]


def scoreboard_month() -> {}:
    scoreboard = {}

    select_users_query = select(users)
    select_users = connection.execute(select_users_query)

    for user in select_users:
        user_scores = select_all_marks_count_per_month_for_user(user[2])
        scoreboard[user[2]] = user_scores[3]

    sorted_scoreboard = sorted(scoreboard.items(), key=lambda x: x[1])
    sorted_scoreboard.reverse()

    return dict(sorted_scoreboard)


def delete_empty_string_for_user(dis_id):
    delete_empty_string_query = marks.delete() \
        .where(marks.c.user_id == get_user_id_by_discord_id(dis_id)) \
        .where(marks.c.morning == False) \
        .where(marks.c.day == False) \
        .where(marks.c.evening == False)
    connection.execute(delete_empty_string_query)


def scoreboard_all():
    pass


# debug command for showing all the data of the tables
def show() -> []:
    def prnt(result):
        for row in result:
            print(f"{row}")

    s_00 = select(users)
    s_01 = select(marks)

    res_1 = connection.execute(s_00)
    res_2 = connection.execute(s_01)

    final = res_1.fetchall() + res_2.fetchall()
    prnt(final)

    return final
