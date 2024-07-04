import pytest
import psycopg2
from decimal import Decimal
from src.class_dbmanager import DBManager


@pytest.fixture()
def db_manager(db_name, params):
    db_manager = DBManager(db_name, params)
    return db_manager


def test_get_companies_and_vacancies_count(db_manager, db_name, params):
    db_manager.get_companies_and_vacancies_count()
    conn = psycopg2.connect(database=db_name, **params)
    cur = conn.cursor()
    try:
        cur.execute("""SELECT company_name, open_vacancies FROM employers ORDER by open_vacancies DESC""")
        res = cur.fetchall()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    assert res == [('МТС', 3640), ('МТС Финтех', 505)]


def test_get_all_vacancies(db_manager, db_name, params):
    db_manager.get_all_vacancies()
    conn = psycopg2.connect(database=db_name, **params)
    cur = conn.cursor()
    try:
        cur.execute("""SELECT v.vacancy_name, e.company_name, v.salary_from, v.salary_to, v.currency, v.url 
                    FROM vacancies as v
                    JOIN employers as e USING(employer_id)""")
        res = cur.fetchall()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    assert res == [('Стажер IT направления', 'МТС', 50000, 0, 'RUR', 'https://hh.ru/vacancy/101287743')]


def test_get_avg_salary(db_manager, db_name, params):
    db_manager.get_avg_salary()
    conn = psycopg2.connect(database=db_name, **params)
    cur = conn.cursor()
    try:
        cur.execute("""SELECT vacancy_name, AVG(salary_to + salary_from) as avg_salary
            FROM vacancies
            GROUP BY vacancy_name
            ORDER BY avg_salary DESC""")
        res = cur.fetchall()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    assert res == [('Python developer (Junior)', Decimal('100000.000000000000')),
                   ('Стажер IT направления', Decimal('50000.000000000000'))]


def get_vacancies_with_keyword(db_manager, db_name, params):
    keyword = 'python'
    db_manager.get_vacancies_with_keyword(keyword)
    conn = psycopg2.connect(database=db_name, **params)
    cur = conn.cursor()
    try:
        cur.execute(f"""SELECT vacancy_name, city, url FROM vacancies 
            WHERE LOWER(vacancy_name) LIKE ('%{keyword.lower()}%')""")
        res = cur.fetchall()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    assert res == [('Python developer (Junior)', 'Владивосток', 'https://hh.ru/vacancy/101219641')]


def test_get_vacancies_with_higher_salary(db_manager, db_name, params):
    db_manager.get_vacancies_with_higher_salary()
    conn = psycopg2.connect(database=db_name, **params)
    cur = conn.cursor()
    try:
        cur.execute("""SELECT vacancy_name, salary_from, salary_to, currency FROM vacancies WHERE salary_to >= 
                (SELECT AVG(salary_to) 
                FROM vacancies) ORDER BY salary_to DESC""")
        res = cur.fetchall()
        conn.commit()
    finally:
        cur.close()
        conn.close()

    assert res == [('Python developer (Junior)', 50000, 50000, 'RUR')]
