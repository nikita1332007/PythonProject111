import psycopg2
from typing import List, Tuple, Optional


class DBManager:
    """
    Класс для работы с базой данных PostgreSQL.
    """

    def __init__(self, user: str, password: int, dbname: str, host='localhost', port=5432):
        """
        Инициализация, подключение к БД.
        """
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=23456,
            host=host,
            port=port
        )

    def add_employer(self, hh_id: str, name: str, url: Optional[str]) -> int:
        """
        Добавляет работодателя в таблицу, если еще не существует.
        :return: id работодателя в таблице
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM employers WHERE hh_id = %s", (hh_id,))
            row = cur.fetchone()
            if row:
                return row[0]

            cur.execute(
                "INSERT INTO employers (hh_id, name, url) VALUES (%s, %s, %s) RETURNING id;",
                (hh_id, name, url)
            )
            employer_id = cur.fetchone()[0]
            self.conn.commit()
            return employer_id

    def add_vacancy(self, hh_id: str, employer_id: int, title: str,
                    salary_from: Optional[int], salary_to: Optional[int],
                    salary_currency: Optional[str], url: Optional[str]) -> None:
        """
        Добавляет вакансию в таблицу.
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM vacancies WHERE hh_id = %s", (hh_id,))
            if cur.fetchone():
                # Вакансия уже существует
                return
            cur.execute(
                """
                INSERT INTO vacancies (hh_id, employer_id, title, salary_from, salary_to, salary_currency, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
                """,
                (hh_id, employer_id, title, salary_from, salary_to, salary_currency, url)
            )
            self.conn.commit()

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        :return: Список (название компании, количество вакансий)
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT employers.name, COUNT(vacancies.id) 
                FROM employers
                LEFT JOIN vacancies ON employers.id = vacancies.employer_id
                GROUP BY employers.name
                ORDER BY COUNT(vacancies.id) DESC;
            """)
            return cur.fetchall()

    def get_all_vacancies(self) -> List[Tuple[str, str, Optional[int], Optional[int], Optional[str], str]]:
        """
        Получает список всех вакансий с данными:
        (название компании, название вакансии, salary_from, salary_to, currency, url вакансии)
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT e.name, v.title, v.salary_from, v.salary_to, v.salary_currency, v.url
                FROM vacancies v
                JOIN employers e ON v.employer_id = e.id
                ORDER BY e.name;
            """)
            return cur.fetchall()

    def get_avg_salary(self) -> Optional[float]:
        """
        Получает среднюю зарплату по вакансиям (по среднему между salary_from и salary_to)
        """
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT AVG((COALESCE(salary_from,0) + COALESCE(salary_to,0))/NULLIF((CASE WHEN salary_from IS NOT NULL AND salary_to IS NOT NULL THEN 2 ELSE 1 END), 0)) 
                FROM vacancies 
                WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL;
            """)
            avg = cur.fetchone()[0]
            return avg

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, str, Optional[int], Optional[int], Optional[str], str]]:
        """
        Получает список вакансий с зарплатой выше средней.
        """
        avg_salary = self.get_avg_salary()
        if avg_salary is None:
            return []

        with self.conn.cursor() as cur:
            cur.execute("""
                   SELECT e.name, v.title, v.salary_from, v.salary_to, v.salary_currency, v.url
                   FROM vacancies v
                   JOIN employers e ON v.employer_id = e.id
                   WHERE (
                       CASE
                           WHEN v.salary_from IS NOT NULL AND v.salary_to IS NOT NULL THEN (v.salary_from + v.salary_to) / 2
                           WHEN v.salary_from IS NOT NULL THEN v.salary_from
                           WHEN v.salary_to IS NOT NULL THEN v.salary_to
                           ELSE 0
                       END
                   ) > %s
                   ORDER BY e.name;
               """, (avg_salary,))
            return cur.fetchall()