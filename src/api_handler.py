import requests
from typing import List, Dict, Optional


class HHApiClient:
    """
    Класс для работы с публичным API hh.ru
    """
    BASE_URL = "https://api.hh.ru"

    def __init__(self):
        self.session = requests.Session()

    def get_employers(self, employer_ids: List[str]) -> List[Dict]:
        """
        Получает данные о работодателях по списку их id
        :param employer_ids: Список id компаний
        :return: Список с информацией о компаниях
        """
        employers = []
        for emp_id in employer_ids:
            url = f"{self.BASE_URL}/employers/{emp_id}"
            resp = self.session.get(url)
            if resp.status_code == 200:
                employers.append(resp.json())
        return employers

    def get_vacancies(self, employer_id: str, per_page=100) -> List[Dict]:
        """
        Получает вакансии указанного работодателя
        :param employer_id: ID компании
        :param per_page: Кол-во вакансий на странице (максимум 100)
        :return: Список вакансий компании
        """
        url = f"{self.BASE_URL}/vacancies"
        params = {
            'employer_id': employer_id,
            'per_page': per_page,
            'page': 0
        }
        vacancies = []

        while True:
            resp = self.session.get(url, params=params)
            if resp.status_code != 200:
                break
            data = resp.json()
            vacancies.extend(data['items'])
            if params['page'] >= data['pages'] - 1:
                break
            params['page'] += 1
        return vacancies

    @staticmethod
    def parse_salary(salary_data: Optional[Dict]) -> (Optional[int], Optional[int], Optional[str]):
        """
        Парсит информацию о зарплате из вакансии
        :param salary_data: зарплата из вакансии (словарь)
        :return: кортеж (min_salary, max_salary, currency)
        """
        if not salary_data:
            return None, None, None
        return salary_data.get('from'), salary_data.get('to'), salary_data.get('currency')