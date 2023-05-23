import csv
import random
import sqlite3
from time import sleep
import requests
from bs4 import BeautifulSoup


def random_sleep():
    sleep(random.randint(1, 5))


def get_page_content(page: int, size: int = 10) -> str:
    query_parameters = {
        'indexName': 'auto,order_auto,newauto_search',
        'country.import.usa.not': '-1',
        'price.currency': '1',
        'abroad.not': '-1',
        'custom.not': '-1',
        'page': page,
        'size': size
    }

    base_url = 'https://auto.ria.com/uk/search/'
    res = requests.get(base_url, params=query_parameters)
    res.raise_for_status()
    return res.text


def get_car_content(data_link_to_view):
    base_url = 'https://auto.ria.com/uk'
    res = requests.get(base_url+data_link_to_view)
    res.raise_for_status()
    return res.text


def get_technical_info(data_link_to_view):
    soup = BeautifulSoup(get_car_content(data_link_to_view), features="html.parser")
    if data_link_to_view[1:8] == 'newauto':
        search_results = soup.find("dl", {"class": "defines_list mb-15 unstyle"})
        engine = extract_newauto_data(search_results, 'Двигун')
        drive_unit = extract_newauto_data(search_results, 'Привід')
        technical_condition = 'New'
        color = extract_newauto_data(search_results, 'Колір кузова')
    else:
        search_results = soup.find("div", {"class": "technical-info", "id": "details"})
        engine = extract_data(search_results, 'Двигун')
        drive_unit = extract_data(search_results, 'Привід')
        technical_condition = extract_data(search_results, 'Технічний стан')
        color = extract_data(search_results, 'Колір')
    random_sleep()
    return [engine, drive_unit, technical_condition, color]


def extract_data(search_results, record: str) -> str:
    try:
        return search_results.find('span', string=record).parent.find('span', {'class': 'argument'}).text
    except AttributeError as e:
        print(e)
        return ''


def extract_newauto_data(search_results, record: str) -> str:
    try:
        return search_results.find('dt', string=record).next_sibling.next_sibling.text
    except AttributeError as e:
        print(e)
        return ''


class CSVWriter:
    def __init__(self, filename, headers):
        self.filename = filename
        self.headers = headers
        with open(self.filename, 'w', encoding='UTF8') as f:
            writer = csv.writer(f)
            writer.writerow(self.headers)

    def write(self, row: list):
        with open(self.filename, 'a', encoding='UTF8') as f:
            writer = csv.writer(f)
            writer.writerow(row)


class SQLWriter:
    def __init__(self, filename, tablename, headers):
        self.filename = filename
        self.tablename = tablename
        self.headers = headers
        self.con = sqlite3.connect(self.filename)
        self.cur = self.con.cursor()

        create = f'''
        CREATE TABLE IF NOT EXISTS {self.tablename} (
            car_id INT,
            data_link_to_view varchar(255),
            engine varchar(255),
            drive_unit varchar(64),
            technical_condition varchar(64),
            color varchar(64)
        );
        '''
        self.cur.execute(create)
        self.con.commit()
        self.con.close()

    def write(self, row: list):
        values = ''
        for item in row:
            values += f"'{item}', "
        sql = f'''
        INSERT INTO {self.tablename}
        VALUES ({values[:-2]});
        '''
        print(sql)
        self.con = sqlite3.connect(self.filename)
        self.cur = self.con.cursor()
        self.cur.execute(sql)
        self.con.commit()
        self.con.close()


def main():
    page = 0
    car_ids = set()

    writers = (
        CSVWriter('cars.csv', ['car_id', 'data_link_to_view', 'engine', 'drive_unit', 'technical_condition', 'color']),
        CSVWriter('cars2.csv', ['car_id', 'data_link_to_view', 'engine', 'drive_unit', 'technical_condition', 'color']),
        SQLWriter('cars.db', 'cars', ['car_id', 'data_link_to_view', 'engine', 'drive_unit', 'technical_condition', 'color'])
    )
    breakpoint()

    while True:
        print(f'Page: {page}')

        page_content = get_page_content(page)
        page += 1
        soup = BeautifulSoup(page_content, features="html.parser")
        search_results = soup.find("div", {"id": "searchResults"})
        ticket_items = search_results.find_all("section", {"class": "ticket-item"})

        if not ticket_items:
            break

        for ticket_item in ticket_items:
            car_details = ticket_item.find("div", {"class": "hide"})
            car_id = car_details["data-id"]
            car_ids.add(car_id)
            data_link_to_view = car_details["data-link-to-view"]
            technical_info = get_technical_info(data_link_to_view)
            for writer in writers:
                writer.write([car_id, data_link_to_view] + technical_info)
        random_sleep()


if __name__ == '__main__':
    main()
