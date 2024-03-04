import psycopg2


class DBManager:
    """
    Класс, который будет подключаться к БД PostgreSQL
    и получать данные из таблиц, согласно заданным методам.
    """

    def __init__(self, database_name: str, params: dict) -> None:
        """Инициализация агрументов Класса DBManager"""
        self.params = params
        self.database_name = database_name
        self.connection = None
        self.cursor = None

    def connect(self):
        """Подключает к БД PostgreSQL"""
        try:
            self.connection = psycopg2.connect(dbname=self.database_name, **self.params)
            # self.cursor = self.connection.cursor()
            print("Успешное подключение к базе данных")
        except psycopg2.Error:
            print(f"Ошибка подключения к базе данных: {self.database_name}")

    def disconnect(self) -> None:
        """Отключает от БД PostgreSQL"""
        # self.cursor.close()
        self.connection.close()
        print("Отключение от базы данных")

    def get_companies_and_vacancies_count(self):
        """
        Метод, который получает список всех компаний и количество вакансий у каждой компании.
        """
        self.cursor = self.connection.cursor()
        self.cursor.execute("""SELECT employer_name, COUNT(*) AS count_vacancies
        FROM vacancies
        INNER JOIN employers USING(employer_id)
        GROUP BY employer_name
        ORDER BY employer_name""")
        rows = self.cursor.fetchall()
        for row in rows:
            print(*row)
        self.cursor.close()

    def get_all_vacancies(self):
        """
        Метод, который, получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию.
        """
        self.cursor = self.connection.cursor()
        self.cursor.execute("""SELECT employer_name, vacancy_title, salary_from, salary_to, vacancy_url
        FROM vacancies
        INNER JOIN employers USING(employer_id)""")
        rows = self.cursor.fetchall()
        for row in rows:
            print(*row)
        self.cursor.close()

    def get_avg_salary(self):
        """
        Метод, который получает среднюю зарплату по вакансиям
        """
        self.cursor = self.connection.cursor()
        self.cursor.execute("""SELECT AVG(salary_from)
            FROM vacancies""")
        result = self.cursor.fetchone()
        result_1 = int(result[0])
        print(f"Cредняя зарплата по всем вакансиям - {result_1} руб.")
        self.cursor.close()

    def get_vacancies_with_higher_salary(self):
        """
        Метод, который получает список всех вакансий,
        у которых зарплата выше средней по всем вакансиям.
        """
        self.cursor = self.connection.cursor()
        self.cursor.execute("""SELECT employer_name, vacancy_title, city, salary_from, salary_to, vacancy_url 
        FROM vacancies
        INNER JOIN employers USING(employer_id)
        WHERE salary_from > ANY (SELECT AVG(salary_from) FROM vacancies)
        ORDER BY salary_from DESC""")
        rows = self.cursor.fetchall()
        for row in rows:
            print(*row)
        self.cursor.close()

    def get_vacancies_with_keyword(self, keyword: str):
        """
        Метод, который получает список всех вакансий,
        в названии которых содержатся переданные в метод слова, например python.
        """
        self.cursor = self.connection.cursor()
        self.cursor.execute(f"""SELECT employer_name, vacancy_title, city, vacancy_url, salary_from, salary_to 
        FROM vacancies
        INNER JOIN employers USING(employer_id)
        WHERE vacancy_title ILIKE '%{keyword}%'""")
        rows = self.cursor.fetchall()
        if len(rows) == 0:
            print("К сожалению, по введенному Вами слову вакансий найдено не было, попробуйте ещё раз.")
        for row in rows:
            print(*row)
        self.cursor.close()