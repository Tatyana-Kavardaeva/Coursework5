import psycopg2
from tabulate import tabulate


class DBManager:

    dbname: str
    params: dict

    def __init__(self, dbname, params):
        self.dbname = dbname
        self.params = params

    def get_companies_and_vacancies_count(self):
        """ Получает список всех компаний и количество вакансий у каждого работодателя """
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT company_name, open_vacancies FROM employers ORDER by open_vacancies DESC""")
            rows = cur.fetchall()

        conn.commit()
        conn.close()
        headers = ["Company name", "Count of vacancies"]
        result = tabulate(rows, headers=headers)
        return result

    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию
        """
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT v.vacancy_name, e.company_name, v.salary_from, v.salary_to, v.currency, v.url 
                    FROM vacancies as v
                    JOIN employers as e USING(employer_id)""")
            rows = cur.fetchall()
        conn.commit()
        conn.close()
        headers = ["Vacancy name", "Company", "Зарплата от", "Зарплата до", "Валюта", "Cсылка на вакансию"]
        result = tabulate(rows, headers=headers)
        return result

    def get_avg_salary(self):
        """ Получает среднюю зарплату по вакансиям """
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT vacancy_name, AVG(salary_to + salary_from) as avg_salary 
            FROM vacancies 
            GROUP BY vacancy_name
            ORDER BY avg_salary DESC""")
            rows = cur.fetchall()
        conn.commit()
        conn.close()
        headers = ["Vacancy name", "Higher_salary"]
        result = tabulate(rows, headers=headers)
        return result

    def get_vacancies_with_higher_salary(self):
        """ Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT vacancy_name, salary_from, salary_to, currency FROM vacancies WHERE salary_to > (SELECT AVG(salary_from + salary_to) 
            FROM vacancies) ORDER BY salary_to DESC""")
            rows = cur.fetchall()
        conn.commit()
        conn.close()
        headers = ["Vacancy name", "salary_from", "salary_to", "currency"]
        result = tabulate(rows, headers=headers)
        return result

    def get_vacancies_with_keyword(self, keyword):
        """ Получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python """
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute(f"""SELECT vacancy_name, city, url FROM vacancies WHERE vacancy_name LIKE ('%{keyword}%')""")
            rows = cur.fetchall()
        conn.commit()
        conn.close()
        headers = ["Vacancy name", "city", "url"]
        result = tabulate(rows, headers=headers)
        return result
