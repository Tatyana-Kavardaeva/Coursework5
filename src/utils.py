import requests
import psycopg2
import time


def get_employers(url, text, area=113, page=0, per_page=100):
    """ Получение списка работодателей по ключевым словам с помощью API hh.ru """
    employers = []
    for word in text:
        params = {'text': word, 'area': area, 'page': page, 'per_page': per_page}
        while True:
            response = requests.get(url, params=params)
            data = response.json()
            res = data.get('items')
            if not res:
                break
            employers.extend(res)
            params['page'] += 1
            time.sleep(2)

    return employers


def get_vacancies(url, employer_id, area=113, page=0, per_page=50):
    """ Получение списка вакансий по id работодателя с помощью API hh.ru """
    vacancies = []
    # for employer_id in emp_id:
    params = {'employer_id': employer_id, 'area': area, 'page': page, 'per_page': per_page}
    while True:
        response = requests.get(url, params=params)
        data = response.json()
        res = data.get('items')
        if not res:
            break
        vacancies.extend(res)
        params['page'] += 1
        time.sleep(2)

    return vacancies


def create_database(db_name: str, params: dict) -> None:
    """ Создание базы данных и таблиц в ней """
    conn = psycopg2.connect(database='postgres', **params)
    cur = conn.cursor()
    conn.autocommit = True

    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")

    cur.close()
    conn.close()

    conn = psycopg2.connect(database=db_name, **params)

    with conn.cursor() as cur:
        cur.execute("CREATE TABLE employers ("
                    # "id serial PRIMARY KEY ,"
                    "employer_id integer PRIMARY KEY,"
                    "company_name varchar,"
                    "open_vacancies integer,"
                    # "city varchar,"
                    "url varchar)")

    with conn.cursor() as cur:
        cur.execute("CREATE TABLE vacancies ("
                    # "id serial PRIMARY KEY,"
                    "vacancy_id integer PRIMARY KEY,"
                    "vacancy_name varchar,"
                    "salary_from integer DEFAULT 0,"
                    "salary_to integer DEFAULT 0,"                    
                    "currency varchar,"                    
                    "published_at date,"
                    "city varchar,"
                    "employer_id integer,"
                    "url varchar)")

    conn.commit()
    conn.close()


def save_data_to_employers(data: list[dict], db_name: str, params: dict):
    """ Сохранение данных о работодателях и вакансиях в базу данных """
    conn = psycopg2.connect(database=db_name, **params)

    with conn.cursor() as cur:
        # cur.execute("TRUNCATE TABLE vacancies")
        # cur.execute("TRUNCATE TABLE employers")

        for employer in data:
            cur.execute(
                "INSERT INTO employers (employer_id, company_name, open_vacancies, url) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (employer_id) DO UPDATE SET company_name = EXCLUDED.company_name, url = EXCLUDED.url",
                (employer.get('id'), employer.get('name'), employer.get('open_vacancies'), employer.get('alternate_url')))

    conn.commit()
    conn.close()


def save_data_to_vacancies(data: list[dict], db_name: str, params: dict):
    """ Сохранение данных о работодателях и вакансиях в базу данных """
    conn = psycopg2.connect(database=db_name, **params)

    with conn.cursor() as cur:
        # cur.execute("TRUNCATE TABLE vacancies")
        # cur.execute("TRUNCATE TABLE employers")

        salary_from = 0
        salary_to = 0
        currency = None

        for vacancy in data:
            employer = vacancy.get('employer')
            vac_city = vacancy.get('area')
            if not vacancy['salary']:
                salary_from = 0
                salary_to = 0
                currency = None
            elif not vacancy['salary']['from']:
                salary_from = 0
            elif not vacancy['salary']['to']:
                salary_to = 0
            else:
                salary_from = vacancy['salary']['from']
                salary_to = vacancy['salary']['to']
                currency = vacancy['salary']['currency']

            cur.execute("INSERT INTO vacancies "
                        "(vacancy_id, vacancy_name, salary_from, salary_to, currency, "
                        "published_at, city, employer_id, url) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (vacancy_id) "
                        "DO UPDATE SET vacancy_name = EXCLUDED.vacancy_name, url = EXCLUDED.url",
                        (vacancy.get('id'), vacancy.get('name'), salary_from, salary_to,
                         currency, vacancy.get('published_at'), vac_city.get('name'),
                         employer.get('id'), vacancy.get('alternate_url')))

    conn.commit()
    conn.close()
