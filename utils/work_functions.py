from typing import Any
import psycopg2
import requests


def get_employers_data(employer_ids: list) -> list[dict]:
    """Функция,для получение данных о работодателях с помощью API hh.ru."""

    employers_data = []
    for employer_id in employer_ids:
        url = f"https://api.hh.ru/employers/{employer_id}"

        params = {
            "employer_id": employer_id,  # строка поиска по employer_id работодателя.
        }
        response = requests.get(url, params=params)
        if response.ok:
            data = response.json()
            employer_info = {
                "name": data["name"],
                "external_id": data["id"],
                "url_hh": data["alternate_url"],
                "site_url": data["site_url"],
                "description": data["description"]
                }
            employers_data.append(employer_info)
        else:
            print("Запрос не выполнен")
            quit()
    return employers_data


def get_vacancies_data(employer_ids: list) -> list[dict]:
    """Получение данных о вакансиях работодателя с помощью API hh.ru."""

    vacancies_data = []
    for employer_id in employer_ids:

        url = "https://api.hh.ru/vacancies"

        params = {
            "per_page": 50,  # количество вакансий на странице.
            "page": None,  # номер страницы результатов.
            "employer_id": employer_id,  # строка поиска по названию вакансии.
        }
        response = requests.get(url, params=params)

        if response.ok:
            data = response.json()
            try:
                for vacancy in data["items"]:
                    vacancy_info = {
                        "employer": vacancy["employer"].get("name"),
                        "employer_id": vacancy["employer"].get("id"),
                        "employer_url": vacancy["employer"].get("alternate_url"),
                        "title": vacancy.get("name"),
                        "vacancy_id": vacancy.get("id"),
                        "location": vacancy["area"].get("name"),
                        "url": vacancy.get("apply_alternate_url"),
                        "salary_from": vacancy["salary"].get("from") if vacancy["salary"] else None,
                        "salary_to": vacancy["salary"].get("to") if vacancy["salary"] else None,
                        "description": vacancy.get("snippet", {}).get("requirement")
                    }
                    if vacancy_info["salary_from"] is None:
                        vacancy_info["salary_from"] = 0

                    if vacancy_info["salary_to"] is None:
                        vacancy_info["salary_to"] = 0

                    if vacancy_info["salary_to"] == 0:
                        vacancy_info["salary_to"] = vacancy_info["salary_from"]

                    vacancies_data.append(vacancy_info)
            except (ValueError, KeyError):
                print("Запрос не удался, вакансии не получены, ошибки ключа или значения")
        else:
            print("Запрос не выполнен")
            quit()
    return vacancies_data


def create_database(database_name: str, params: dict) -> None:
    """
    Создание базы данных и таблиц для сохранения данных о работодателях и их вакансиях.
    """

    conn = psycopg2.connect(dbname='postgres', **params)
    # Команда для автоматического создания commit после каждой операции
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {database_name}")
    cur.execute(f"CREATE DATABASE {database_name}")

    cur.close()
    conn.close()

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE employers (
                    employer_id SERIAL PRIMARY KEY,
                    employer_name VARCHAR(100) NOT NULL,
                    external_id INTEGER NOT NULL,
                    site_url VARCHAR(150),
                    description TEXT
                )
            """)

    with conn.cursor() as cur:
        cur.execute("""
                CREATE TABLE vacancies (
                      vacancy_id SERIAL PRIMARY KEY,
                      vacancy_title VARCHAR NOT NULL,
                      employer_id INTEGER REFERENCES employers(employer_id),
                      external_id INTEGER NOT NULL,
                      city VARCHAR(50),
                      vacancy_url VARCHAR(150),
                      salary_from DECIMAL,
                      salary_to DECIMAL,
                      description TEXT
                )
            """)

    conn.commit()
    conn.close()


def save_data_to_database(data: list[dict[str, Any]], database_name: str, params: dict) -> None:
    """Сохранение данных о работодателях и их вакансиях в базу данных."""

    conn = psycopg2.connect(dbname=database_name, **params)

    with conn.cursor() as cur:
        for employer in data[0]['employers']:
            # employer_data = employer['employer']
            cur.execute(
                """
                INSERT INTO employers (employer_name, external_id, site_url, description)
                VALUES (%s, %s, %s, %s)
                RETURNING employer_id
                """,
                (employer['name'], employer['external_id'], employer['site_url'],
                 employer['description'])
            )
            employer_id = cur.fetchone()[0]
            employer_name = employer['name']
            for vacancy in data[0]['vacancies']:
                if employer_name == vacancy['employer']:
                    cur.execute(
                        """
                        INSERT INTO vacancies (vacancy_title, employer_id, external_id, city, 
                        vacancy_url, salary_from, salary_to, description)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (vacancy['title'], employer_id, vacancy['vacancy_id'], vacancy['location'],
                         f"https://hh.ru/applicant/vacancy_response?vacancyId={vacancy['vacancy_id']}",
                         vacancy['salary_from'], vacancy['salary_to'], vacancy['description'])
                    )
    conn.commit()
    conn.close()
