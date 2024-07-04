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
            cur.execute("""SELECT company_name, open_vacancies FROM employers ORDER BY open_vacancies DESC""")
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
            cur.execute("""SELECT v.vacancy_name, e.company_name, v.salary_to, v.salary_to, v.currency, v.url 
                    FROM vacancies as v
                    JOIN employers as e USING(employer_id)""")
            rows = cur.fetchall()
        conn.commit()
        conn.close()
        headers = ["Vacancy name", "company name", "salary to", "salary to", "currency", "url"]
        result = tabulate(rows, headers=headers)
        return result

    def get_avg_salary(self):
        """ Получает среднюю зарплату по вакансиям """
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT vacancy_name, AVG(salary_to + salary_from) as higher_salary,  currency
            FROM vacancies 
            WHERE salary_to <> 0 AND salary_from <> 0 AND currency = 'RUR'
            GROUP BY vacancy_name, currency
            ORDER BY higher_salary DESC""")
            rows = cur.fetchall()
        conn.commit()
        conn.close()
        headers = ["Vacancy name", "higher salary", "currency"]
        result = tabulate(rows, headers=headers)
        return result

    def get_vacancies_with_higher_salary(self):
        """ Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям """
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute("""SELECT vacancy_name, AVG(salary_from + salary_to) as higher_salary, currency 
                        FROM vacancies 
                        WHERE salary_to > (SELECT AVG(salary_from + salary_to) FROM vacancies 
                        WHERE salary_from <> 0 AND salary_to <> 0 AND currency = 'RUR')
                        AND salary_from <> 0 AND salary_to <> 0 AND currency = 'RUR'
                        GROUP BY vacancy_name, currency
                        ORDER BY higher_salary DESC""")
            rows = cur.fetchall()
        conn.commit()
        conn.close()
        headers = ["Vacancy name", "higher salary", "currency"]
        result = tabulate(rows, headers=headers)
        return result

    def get_vacancies_with_keyword(self, keyword):
        """ Получает список всех вакансий, в названии которых содержатся переданные в метод слова,
            например 'python' """
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn.cursor() as cur:
            cur.execute(f"""SELECT vacancy_name, city, url FROM vacancies 
            WHERE LOWER(vacancy_name) LIKE ('%{keyword.lower()}%')""")
            rows = cur.fetchall()
        conn.commit()
        conn.close()
        headers = ["Vacancy name", "city", "url"]
        result = tabulate(rows, headers=headers)
        return result
