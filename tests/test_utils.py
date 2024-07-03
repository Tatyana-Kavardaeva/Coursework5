import pytest
import psycopg2
from src.utils import create_database, save_data_to_vacancies, save_data_to_employers


def test_create_database(db_name, params):
    create_database(db_name, params)
    conn = psycopg2.connect(database='postgres', **params)
    cur = conn.cursor()
    try:
        cur.execute("SELECT datname FROM pg_database WHERE datname = %s;", (db_name,))
        res = cur.fetchall()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    assert res == [(db_name,)]


def test_save_data_to_employers(data_employers, db_name, params):
    """ Проверяет запись данных в таблицу "employers" """
    save_data_to_employers(data_employers, db_name, params)
    conn = psycopg2.connect(database=db_name, **params)
    cur = conn.cursor()
    try:
        cur.execute("select count(company_name) as count_emp from employers")
        res = cur.fetchall()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    assert res == [(2,)]


def test_save_data_to_vacancies(data_vacancies, db_name, params):
    """ Проверяет запись данных в таблицу "vacancies" """
    save_data_to_vacancies(data_vacancies, db_name, params)
    conn = psycopg2.connect(database=db_name, **params)
    cur = conn.cursor()
    try:
        cur.execute("select count(vacancy_name) as count_v from vacancies")
        res = cur.fetchall()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    assert res == [(2,)]
