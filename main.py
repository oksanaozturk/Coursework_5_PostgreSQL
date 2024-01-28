from utils.config import config
from utils.work_functions import get_employers_data, get_vacancies_data, create_database, save_data_to_database
from src.db_manager import DBManager


def main():
    employer_ids = [
        3529,  # Сбербанк
        39305,  # Газпром
        907345,  # Газпром
        1373,  # Аэрофлот
        80,  # Альфа-Банк
        4181,  # ВТБ
        4934,  # Вымпелком/Билайн
        2748,  # Ростелеком
        577743,  # Росатом
        3776,  # МТС
        988387  # НЛМК
    ]

    data_all = []
    employers_data = get_employers_data(employer_ids)
    vacancies_data = get_vacancies_data(employer_ids)
    data_all.append({
        "employers": employers_data,
        "vacancies": vacancies_data
    })
    params = config()
    create_database('parser_hh_ru', params)
    save_data_to_database(data_all, 'parser_hh_ru', params)

    print("Привет!\n"
          "Предлагаем Вам ознакомиться с актуальными вакансиями ведущих работодателей России,\n"
          "входящих в 'Топ работодателей-25' за 2023 год.\n"
          "Заработная плата по всем вакансиям указана в рублях.\n")

    db_manager = DBManager('parser_hh_ru', params)
    db_manager.connect()

    while True:
        print("\nНажмите 1, если хотите получить список всех доступных компаний и количества вакансий.\n"
              "Нажмите 2, если хотите получить список всех вакансий с указанием названия компании, названия вакансии,"
              "зарплаты и ссылки на вакансию.\n"
              "Нажмите 3, если хотите получить среднюю зарплату по всем вакансиям.\n"
              "Нажмите 4, если хотите получить список вакансий с зарплатой выше среднего.\n"
              "Нажмите 5, если хотите получить список вакансий по Вашему запросу.\n"
              "Нажмите 6, если хотите завершить сессию.\n")

        user_input = input("Ваш выбор: ")
        if user_input == '1':
            print("Представляем Вашему вниманию список Компаний и количество вакансий для каждой в нашем поиске: \n")
            db_manager.get_companies_and_vacancies_count()

            user_input = input("\nПродолжим? Да/Нет: ")
            if user_input.lower() == 'да':
                continue
            else:
                db_manager.disconnect()
                print("\nСпасибо, что воспользовались нашим сервисом!\n"
                      "Будем рады видеть Вас снова!\n")
                break

        elif user_input == '2':
            print("Представляем Вашему вниманию список всех вакансий Компаний, "
                  "с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию: \n")
            db_manager.get_all_vacancies()

            user_input = input("\nПродолжим? Да/Нет: ")
            if user_input.lower() == 'да':
                continue
            else:
                db_manager.disconnect()
                print("\nСпасибо, что воспользовались нашим сервисом!\n"
                      "Будем рады видеть Вас снова!\n")
                break

        elif user_input == '3':
            print("Представляем Вашему вниманию среднюю зарплату по всем вакансиям: \n")
            db_manager.get_avg_salary()

            user_input = input("\nПродолжим? Да/Нет: ")
            if user_input.lower() == 'да':
                continue
            else:
                db_manager.disconnect()
                print("\nСпасибо, что воспользовались нашим сервисом!\n"
                      "Будем рады видеть Вас снова!\n")
                break

        elif user_input == '4':
            print("Представляем Вашему вниманию список всех вакансий,"
                  "у которых зарплата выше средней (список ранжирован по убыванию), "
                  "с указанием названия компании, вакансии, города, зарплаты и ссылки на вакансию: \n")
            db_manager.get_vacancies_with_higher_salary()

            user_input = input("\nПродолжим? Да/Нет: ")
            if user_input.lower() == 'да':
                continue
            else:
                db_manager.disconnect()
                print("\nСпасибо, что воспользовались нашим сервисом!\n"
                      "Будем рады видеть Вас снова!\n")
                break

        elif user_input == '5':
            keyword = input("Какую профессию будем искать? Введите ключевое слово: ")
            print("\nПредставляем Вашему вниманию список всех вакансий, "
                  "найденных по введенному Вами ключевому слову: \n")
            db_manager.get_vacancies_with_keyword(keyword)

            user_input = input("\nПродолжим? Да/Нет: ")
            if user_input.lower() == 'да':
                continue
            else:
                db_manager.disconnect()
                print("\nСпасибо, что воспользовались нашим сервисом!\n"
                      "Будем рады видеть Вас снова!\n")
                break

        elif user_input == '6':
            db_manager.disconnect()
            print("\nСпасибо, что воспользовались нашим сервисом!\n"
                  "Будем рады видеть Вас снова!\n")
            break

        else:
            print("\nПожалуйста, проверьте, что Вы ввели число от 1 до 6.")
            continue


if __name__ == "__main__":
    main()
