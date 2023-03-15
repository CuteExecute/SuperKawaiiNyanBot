from datetime import datetime

import pytest
from sqlalchemy import create_engine, select, Table, Column, Integer, Boolean, String, MetaData, ForeignKey, func, event
from sqlalchemy.sql import select, and_, extract
from functools import wraps
import os
import sys
import DbHelper

temp_users_count = None
temp_user_id = None
temp_is_date_exists = None


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Data(metaclass=Singleton):
    user_name = 'Nekoglai_test'
    user_dis_id = '1337228_test'

    marks = {
        'scene_1': {'date': '2023-01-01_test', 'mrg': False, 'day': False, 'evg': False},
        'scene_2': {'date': '2023-01-02_test', 'mrg': False, 'day': True, 'evg': True},
        'scene_3': {'date': '2023-01-03_test', 'mrg': False, 'day': False, 'evg': True},
        'scene_4': {'date': '2023-01-04_test', 'mrg': True, 'day': False, 'evg': False},
        'scene_5': {'date': '2023-01-05_test', 'mrg': True, 'day': True, 'evg': False},
        'scene_6': {'date': '2023-01-06_test', 'mrg': True, 'day': True, 'evg': True},
        'scene_7': {'date': f'{datetime.now().date()}', 'mrg': False, 'day': True, 'evg': True},
        'scene_8': {'date': f'{datetime.now().date()}', 'mrg': False, 'day': False, 'evg': True}
    }


def connect_to_db_decorator(func):
    @wraps(func)
    def connect_to_db(*args):
        engine = DbHelper.engine

        users = DbHelper.users
        marks = DbHelper.marks

        connection = engine.connect()

        func(*args, connection, users, marks)

        connection.close()
        engine.dispose()

    return connect_to_db


@connect_to_db_decorator
def user_create(name, dis_id, connection=None, users=None, marks=None):
    def is_user_exists(dis_id):
        is_user_exists_query = select(users).where(users.c.discord_user_id == dis_id)
        result = connection.execute(is_user_exists_query).fetchall()
        return bool(result)

    if not is_user_exists(dis_id):
        insert_user_query = users.insert().values(name=f'{name}', discord_user_id=f'{dis_id}')
        connection.execute(insert_user_query)
    else:
        raise Exception("user is exist!")


@connect_to_db_decorator
def user_delete(dis_id, connection=None, users=None, marks=None):
    delete_user_query = users.delete().where(users.c.discord_user_id == f'{dis_id}')
    connection.execute(delete_user_query)


@connect_to_db_decorator
def get_users_count(connection=None, users=None, marks=None):
    users_count_query = select([func.count()]).select_from(users)
    result = connection.execute(users_count_query).fetchall()
    global temp_users_count
    temp_users_count = int(result[0][0])


@connect_to_db_decorator
def get_user_id_by_discord_id(dis_id, connection=None, users=None, marks=None) -> int:
    id_user_query = select(users.c.id_user).where(users.c.discord_user_id == dis_id)
    res = connection.execute(id_user_query).fetchall()
    global temp_user_id
    temp_user_id = int(res[0][0])


@connect_to_db_decorator
def create_marks_for_user(date, dis_id, mrg=None, day=None, evg=None, connection=None, users=None, marks=None):
    global temp_user_id
    if mrg != None and day != None and evg != None:
        get_user_id_by_discord_id(dis_id)
        create_mrk_query = marks.insert().values(user_id=temp_user_id,
                                                 date=f"{date}",
                                                 morning=mrg,
                                                 day=day,
                                                 evening=evg)
        connection.execute(create_mrk_query)
        temp_user_id = None
    else:
        raise Exception("The variable condition should not be None!")


@connect_to_db_decorator
def delete_or_update_marks_for_user(date, dis_id, mrg=None, day=None, evg=None, connection=None, users=None,
                                    marks=None):
    def delete_empty_string_for_user():
        global temp_user_id
        get_user_id_by_discord_id(dis_id)
        delete_empty_string_query = marks.delete() \
            .where(marks.c.user_id == temp_user_id) \
            .where(marks.c.morning == False) \
            .where(marks.c.day == False) \
            .where(marks.c.evening == False)
        connection.execute(delete_empty_string_query)
        temp_user_id = None

    if mrg != None and day != None and evg != None:
        global temp_user_id
        get_user_id_by_discord_id(dis_id)
        delete_mrk_query = marks.update() \
            .where(marks.c.user_id == temp_user_id) \
            .where(marks.c.date == date) \
            .values(morning=mrg) \
            .values(day=day) \
            .values(evening=evg)
        connection.execute(delete_mrk_query)
        temp_user_id = None

        delete_empty_string_for_user()
    else:
        raise Exception("The variable condition should not be None!")


@connect_to_db_decorator
def is_date_exists_for_user(date, dis_id, connection=None, users=None, marks=None):
    global temp_is_date_exists
    global temp_user_id
    get_user_id_by_discord_id(dis_id)
    is_date_exists_query = select(marks).where(marks.c.date == date) \
        .where(marks.c.user_id == temp_user_id)
    result = connection.execute(is_date_exists_query)
    temp_is_date_exists = bool(result.fetchall())


@connect_to_db_decorator
def show(connection=None, users=None, marks=None) -> []:
    def prnt(result):
        for row in result:
            print(f"{row}")

    s_00 = select(users)
    s_01 = select(marks)

    res_1 = connection.execute(s_00)
    res_2 = connection.execute(s_01)

    final = res_1.fetchall() + res_2.fetchall()
    prnt(final)


########################################################################################################################
# pytest -s -v
# python -m pytest -s -v

# test 01
def test_get_month_by_date_string():
    expected = 1
    actual = DbHelper.get_month_by_date_string('2023-01-01')
    assert expected == actual


# test 02
def test_get_username_by_discord_id():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # test body
        expected = Data.user_name
        actual = DbHelper.get_username_by_discord_id(Data.user_dis_id)

        assert expected == actual
    finally:
        # teardown
        user_delete(Data.user_dis_id)


# test 03
def test_is_user_exists():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # test body
        expected = True
        actual = DbHelper.is_user_exists(Data.user_dis_id)

        assert expected == actual
    finally:
        # teardown
        user_delete(Data.user_dis_id)


# test 04
def test_get_user_id_by_discord_id():
    global temp_users_count
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # test body
        get_users_count()
        expected = temp_users_count

        actual = DbHelper.get_user_id_by_discord_id(Data.user_dis_id)

        assert expected == actual
    finally:
        # teardown
        user_delete(Data.user_dis_id)
        temp_users_count = None


# test 05
def test_user_create():
    try:
        # test body
        DbHelper.user_create(Data.user_name, Data.user_dis_id)

        expected = True
        actual = DbHelper.is_user_exists(Data.user_dis_id)
        assert expected == actual
    finally:
        # teardown
        user_delete(Data.user_dis_id)


# test 06
def test_user_update():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # test body
        new_user_name = 'IvanZolo_test'
        expected = 'IvanZolo_test'

        DbHelper.user_update(Data.user_dis_id, new_user_name)
        actual = DbHelper.get_username_by_discord_id(Data.user_dis_id)

        assert expected == actual
    finally:
        # teardown
        user_delete(Data.user_dis_id)


# test 07
def test_user_delete():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # test body
        DbHelper.user_delete(Data.user_dis_id)
        expected = False
        actual = DbHelper.is_user_exists(Data.user_dis_id)
        assert expected == actual
    finally:
        # teardown
        user_delete(Data.user_dis_id)


# test 08
def test_user_clear_all_marks():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_1']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_1']['mrg'],
                              Data.marks['scene_1']['day'],
                              Data.marks['scene_1']['evg'])

        # create second mark for user
        create_marks_for_user(Data.marks['scene_2']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_2']['mrg'],
                              Data.marks['scene_2']['day'],
                              Data.marks['scene_2']['evg'])

        # test body
        expected = True

        actual = DbHelper.is_date_exists_for_user(Data.marks['scene_1']['date'], Data.user_dis_id)
        assert expected == actual

        actual = DbHelper.is_date_exists_for_user(Data.marks['scene_2']['date'], Data.user_dis_id)
        assert expected == actual
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_1']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_2']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 09
def test_is_date_exists_for_user():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_3']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_3']['mrg'],
                              Data.marks['scene_3']['day'],
                              Data.marks['scene_3']['evg'])

        # test body
        expected = True
        actual = DbHelper.is_date_exists_for_user(Data.marks['scene_3']['date'], Data.user_dis_id)
        assert expected == actual
    finally:
        # tear down
        delete_or_update_marks_for_user(Data.marks['scene_3']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 10
def test_mark_set_for_user_by_date():
    try:
        # set up
        event_time = 'day'
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_3']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_3']['mrg'],
                              Data.marks['scene_3']['day'],
                              Data.marks['scene_3']['evg'])

        # test body
        DbHelper.mark_set_for_user_by_date(Data.user_name,
                                           Data.user_dis_id,
                                           event_time,
                                           Data.marks['scene_3']['date'])

        expected = True
        actual = DbHelper.is_date_exists_for_user(Data.marks['scene_3']['date'], Data.user_dis_id)

        assert expected == actual
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_3']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 11
def test_mark_remove_for_user_by_date():
    try:
        # set up
        event_time = 'mrg'

        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_4']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_4']['mrg'],
                              Data.marks['scene_4']['day'],
                              Data.marks['scene_4']['evg'])

        # create second mark for user
        create_marks_for_user(Data.marks['scene_5']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_5']['mrg'],
                              Data.marks['scene_5']['day'],
                              Data.marks['scene_5']['evg'])
        # test body
        DbHelper.mark_remove_for_user_by_date(Data.user_dis_id, event_time, Data.marks['scene_4']['date'])

        expected = False
        actual = DbHelper.is_date_exists_for_user(Data.marks['scene_4']['date'], Data.user_dis_id)

        assert expected == actual

        expected = True
        actual = DbHelper.is_date_exists_for_user(Data.marks['scene_5']['date'], Data.user_dis_id)

        assert expected == actual
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_4']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_5']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 12
def test_select_marks_by_user_per_month():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_7']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_7']['mrg'],
                              Data.marks['scene_7']['day'],
                              Data.marks['scene_7']['evg'])

        # create second mark for user
        create_marks_for_user(Data.marks['scene_8']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_8']['mrg'],
                              Data.marks['scene_8']['day'],
                              Data.marks['scene_8']['evg'])
        # test body
        expected = [(Data.user_name,
                     Data.marks['scene_7']['date'],
                     Data.marks['scene_7']['mrg'],
                     Data.marks['scene_7']['day'],
                     Data.marks['scene_7']['evg']),
                    (Data.user_name,
                     Data.marks['scene_8']['date'],
                     Data.marks['scene_8']['mrg'],
                     Data.marks['scene_8']['day'],
                     Data.marks['scene_8']['evg'])]
        actual = DbHelper.select_marks_by_user_per_month(Data.user_dis_id).fetchall()
        assert expected == actual
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_7']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_8']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 13
def test_select_marks_by_user_all_date():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_7']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_7']['mrg'],
                              Data.marks['scene_7']['day'],
                              Data.marks['scene_7']['evg'])

        # create second mark for user
        create_marks_for_user(Data.marks['scene_8']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_8']['mrg'],
                              Data.marks['scene_8']['day'],
                              Data.marks['scene_8']['evg'])

        # create third mark for user
        create_marks_for_user(Data.marks['scene_4']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_4']['mrg'],
                              Data.marks['scene_4']['day'],
                              Data.marks['scene_4']['evg'])

        # create fourth mark for user
        create_marks_for_user(Data.marks['scene_5']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_5']['mrg'],
                              Data.marks['scene_5']['day'],
                              Data.marks['scene_5']['evg'])

        # test body
        expected = [(Data.user_name,
                     Data.marks['scene_7']['date'],
                     Data.marks['scene_7']['mrg'],
                     Data.marks['scene_7']['day'],
                     Data.marks['scene_7']['evg']),
                    (Data.user_name,
                     Data.marks['scene_8']['date'],
                     Data.marks['scene_8']['mrg'],
                     Data.marks['scene_8']['day'],
                     Data.marks['scene_8']['evg']),
                    (Data.user_name,
                     Data.marks['scene_4']['date'],
                     Data.marks['scene_4']['mrg'],
                     Data.marks['scene_4']['day'],
                     Data.marks['scene_4']['evg']),
                    (Data.user_name,
                     Data.marks['scene_5']['date'],
                     Data.marks['scene_5']['mrg'],
                     Data.marks['scene_5']['day'],
                     Data.marks['scene_5']['evg'])]
        actual = DbHelper.select_marks_by_user_all_date(Data.user_dis_id).fetchall()
        assert expected == actual
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_7']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_8']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_4']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_5']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 14
def test_select_all_marks_count_per_month_for_user():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_7']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_7']['mrg'],
                              Data.marks['scene_7']['day'],
                              Data.marks['scene_7']['evg'])

        # create second mark for user
        create_marks_for_user(Data.marks['scene_8']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_8']['mrg'],
                              Data.marks['scene_8']['day'],
                              Data.marks['scene_8']['evg'])
        # test body
        expected = [0, 1, 2, 3]
        actual = DbHelper.select_all_marks_count_per_month_for_user(Data.user_dis_id)
        assert expected == actual
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_7']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_8']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 15
def test_select_all_marks_count_all_time_for_user():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_3']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_3']['mrg'],
                              Data.marks['scene_3']['day'],
                              Data.marks['scene_3']['evg'])

        # create second mark for user
        create_marks_for_user(Data.marks['scene_4']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_4']['mrg'],
                              Data.marks['scene_4']['day'],
                              Data.marks['scene_4']['evg'])

        # test body
        expected = [1, 0, 1, 2]
        actual = DbHelper.select_all_marks_count_all_time_for_user(Data.user_dis_id)
        assert expected == actual
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_3']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_4']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 16
def test_scoreboard_month():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_7']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_7']['mrg'],
                              Data.marks['scene_7']['day'],
                              Data.marks['scene_7']['evg'])

        # create second mark for user
        create_marks_for_user(Data.marks['scene_8']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_8']['mrg'],
                              Data.marks['scene_8']['day'],
                              Data.marks['scene_8']['evg'])

        # test body
        all_marks_count = DbHelper.select_all_marks_count_per_month_for_user(Data.user_dis_id)
        expected_key = Data.user_dis_id
        expected = {f'{Data.user_dis_id}': all_marks_count[-1]}
        actual = DbHelper.scoreboard_month()
        assert expected_key in actual.keys() and expected['1337228_test'] == actual.get('1337228_test')
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_7']['date'], Data.user_dis_id, False, False, False)
        delete_or_update_marks_for_user(Data.marks['scene_8']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)


# test 17
def test_delete_empty_string_for_user():
    try:
        # set up
        user_create(Data.user_name, Data.user_dis_id)

        # create first mark for user
        create_marks_for_user(Data.marks['scene_1']['date'],
                              Data.user_dis_id,
                              Data.marks['scene_1']['mrg'],
                              Data.marks['scene_1']['day'],
                              Data.marks['scene_1']['evg'])

        # test body
        DbHelper.delete_empty_string_for_user(Data.user_dis_id)

        expected = False
        actual = DbHelper.is_date_exists_for_user(Data.marks['scene_1']['date'], Data.user_dis_id)
        assert expected == actual
    finally:
        # teardown
        delete_or_update_marks_for_user(Data.marks['scene_1']['date'], Data.user_dis_id, False, False, False)
        user_delete(Data.user_dis_id)