import os
from api_handler import HHApiClient
from db_setup import create_database, create_tables
from db_manager import DBManager

def main():
    print("Добро пожаловать в приложение для выгрузки вакансий с hh.ru и управления БД.")

    DB_NAME = os.getenv('DB_NAME', 'hh_database')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', '23456')
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '5432'))

    create_database(DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)

    db_manager = DBManager(DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT)

    create_tables(db_manager.conn)

    hh_client = HHApiClient()

    while True:
        print("\nМеню:")
        print("1. Загрузить данные о компаниях и вакансиях")
        print("2. Посмотреть количество вакансий по каждой компании")
        print("3. Посмотреть все вакансии")
        print("4. Посмотреть среднюю зарплату по вакансиям")
        print("5.Посмотреть вакансии с зарплатой выше средней")
        print("6. Выход")

        choice = input("Выберите пункт меню (1-6): ").strip()
        if choice == '1':
            # Запрашиваем ID компаний у пользователя (через запятую)
            company_ids_str = input("Введите ID компаний hh.ru через запятую (например, 12345,67890): ").strip()
            if not company_ids_str:
                print("Ошибка: введена пустая строка.")
                continue

            company_ids = [c.strip() for c in company_ids_str.split(',') if c.strip()]
            if not company_ids:
                print("Ошибка: нет корректных ID компаний.")
                continue

            print("Начинаем загрузку данных...")
            for comp_id in company_ids:
                companies = hh_client.get_employers([comp_id])
                if not companies:
                    print(f"Компания с ID {comp_id} не найдена или ошибка при запросе.")
                    continue
                company = companies[0]
                employer_id = db_manager.add_employer(
                    hh_id=company['id'],
                    name=company['name'],
                    url=company.get('alternate_url')
                )

                vacancies = hh_client.get_vacancies(company['id'])
                for vac in vacancies:
                    salary_from, salary_to, currency = hh_client.parse_salary(vac.get('salary'))
                    db_manager.add_vacancy(
                        hh_id=vac['id'],
                        employer_id=employer_id,
                        title=vac['name'],
                        salary_from=salary_from,
                        salary_to=salary_to,
                        salary_currency=currency,
                        url=vac.get('alternate_url')
                    )
                print(f"Загружено данных по компании: {company['name']} (вакансий: {len(vacancies)})")
            print("Загрузка данных завершена.")

        elif choice == '2':
            data = db_manager.get_companies_and_vacancies_count()
            print("\nКомпании и количество вакансий:")
            for comp_name, vac_count in data:
                print(f"{comp_name}: {vac_count}")

        elif choice == '3':
            vacancies = db_manager.get_all_vacancies()
            print("\nВсе вакансии:\n")
            for comp_name, title, salary_from, salary_to, currency, url in vacancies:
                salary_str = "не указана"
                if salary_from or salary_to:
                    salary_str = f"{salary_from or ''} - {salary_to or ''} {currency or ''}".strip()
                print(f"{comp_name} | {title} | Зарплата: {salary_str} | Ссылка: {url}")

        elif choice == '4':
            avg_salary = db_manager.get_avg_salary()
            if avg_salary:
                print(f"\nСредняя зарплата по вакансиям: {avg_salary:.2f}")
            else:
                print("\nНет доступных данных для расчёта средней зарплаты.")

        elif choice == '5':
            vacancies = db_manager.get_vacancies_with_higher_salary()
            if not vacancies:
                print("\nНет вакансий с зарплатой выше средней или нет данных.")
            else:
                print("\nВакансии с зарплатой выше средней:")
                for comp_name, title, salary_from, salary_to, currency, url in vacancies:
                    salary_str = "не указана"
                    if salary_from or salary_to:
                        salary_str = f"{salary_from or ''} - {salary_to or ''} {currency or ''}".strip()
                    print(f"{comp_name} | {title} | Зарплата: {salary_str} | Ссылка: {url}")

        elif choice == '6':
            print("Выход из программы.")
            break
        else:
            print("Неверный ввод, попробуйте снова.")


if __name__ == "__main__":
    main()