from src.utils import get_employers, get_vacancies, create_database, save_data_to_vacancies, save_data_to_employers
from src.class_dbmanager import DBManager
from config import config


def main():
    # # Запрашиваем список компаний
    # companies = input("Введите ключевые слова для поиска компаний: ").split(', ')
    # # Получаем список работодателей с hh.ru по ключевым словам
    # employers = get_employers('https://api.hh.ru/employers', companies)
    # print(f"Получены данные о {len(employers)} работодателях.\n")
    # for e in employers:
    #     print(f"{e.get('name')}: id работодателя: {e.get('id')}, "
    #           f"количество открытых вакансий: {e.get('open_vacancies')}")
    # print()
    # # Запрашиваем список id работодателей для поиска вакансий
    # company_ids = input("Введите id работодателя/ей для поиска вакансий: ").split(', ')
    # # Получаем данные о вакансиях
    # dict_vac = {}
    # for com_id in company_ids:
    #     vacancies = get_vacancies('https://api.hh.ru/vacancies', com_id)
    #     print(f"Получены данные от {com_id} о {len(vacancies)} вакансиях")
    #     dict_vac[com_id] = vacancies
    # # Создаем базу данных
    params = config()
    print()
    db_name = input("Введите название базы данных: ")
    # create_database(db_name, params)
    # print(f"База данных '{db_name}' создана")
    # # Сохраняем данные о работодателях
    # save_data_to_employers(employers, db_name, params)
    # print("Данные о работодателях успешно сохранены в БД")
    # # Сохраняем данные о вакансиях
    # for k, v in dict_vac.items():
    #     save_data_to_vacancies(v, db_name, params)
    # print("Данные о вакансиях успешно сохранены в БД")

    db_manager = DBManager(db_name, params)
    # print(db_manager.get_companies_and_vacancies_count())
    # print(db_manager.get_all_vacancies())
    # print(db_manager.get_avg_salary())
    # print(db_manager.get_vacancies_with_higher_salary())
    print(db_manager.get_vacancies_with_keyword('python'))

    # rows = db_manager.get_all_vacancies()
    # print(len(rows))
    # for row in rows:
    #     print(row)


    # print(len(employers))
    # print(len(vacancies))
    # for v in vacancies:
    #     print(f"{v['employer']['id']} {v.get('id')} {v.get('name')} {v.get('alternate_url')}")


if __name__ == "__main__":
    main()
