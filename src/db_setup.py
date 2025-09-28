import psycopg2
import os


def create_database(db_name='postgres', user="nikita", password="23456", host='localhost', port=5432) -> None:
    """
    Создает базу данных PostgreSQL при отсутствии
    """
    try:
        conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}';")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f'CREATE DATABASE {db_name};')
            print(f"База данных '{db_name}' создана.")
        else:
            print(f"База данных '{db_name}' уже существует.")

        cur.close()
        conn.close()

    except psycopg2.Error as e:
        print("Ошибка при создании базы данных:", e)


def create_tables(conn) -> None:
    """
    Создает таблицы employers и vacancies
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS employers (
                id SERIAL PRIMARY KEY,
                hh_id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(255) NOT NULL,
                url TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                hh_id VARCHAR(50) UNIQUE NOT NULL,
                employer_id INT REFERENCES employers(id),
                title VARCHAR(255) NOT NULL,
                salary_from INT,
                salary_to INT,
                salary_currency VARCHAR(10),
                url TEXT
            );
        """)
        conn.commit()
        print("Таблицы созданы (если не существовали).")