import requests
from ftplib import FTP
import psycopg2
import psycopg2.extras
import pandas as pd
from loguru import logger
from api_connectors import *
from config import *

logger.remove()
logger.add(sink='logfile.log', format="{time} {level} {message}", level="INFO")

# для теста (в дальнейшем нужно подтягивать информацию из соотв. таблиц, account_list, wh_table)
test_account_id = 105
test_warehouse_id = 777


def read_from_suppliers_db(client_id):  # подсоединиться к БД и получить список поставщиков для клиента с client_id
    try:
        connection = psycopg2.connect(LOCAL_DB_DSN)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        sql = '''
        SELECT * FROM suppliers
        JOIN supplier_client ON suppliers.id=supplier_client.supplier_id
        WHERE supplier_client.client_id=%s AND supplier_client.last_request_date<CURRENT_DATE
        '''\
              % str(client_id)

        cursor.execute(sql)
        suppliers = cursor.fetchall()
        return suppliers
    except ConnectionError as error:
        print(f'Ошибка при подключении или чтении из таблицы suppliers: {error}')
        logger.error(f'Ошибка при подключении или чтении из таблицы suppliers: {error}')


def update_last_request_date(client_id, supplier_id):  # обновить дату last_request_date
    try:
        connection = psycopg2.connect(LOCAL_DB_DSN)
        connection.autocommit = True
        cursor = connection.cursor()

        sql = '''
        UPDATE supplier_client
        SET last_request_date=CURRENT_DATE
        WHERE client_id=%s AND supplier_id=%s 
        '''\
              % (str(client_id), str(supplier_id))

        cursor.execute(sql)
    except ConnectionError as error:
        print(f'Ошибка при обновлении даты в таблице suppliers: {error}')
        logger.error(f'Ошибка при обновлении даты в таблице suppliers: {error}')


def show_client_list():  # подсоединиться к БД и получить список поставщиков
    try:
        connection = psycopg2.connect(LOCAL_DB_DSN)
        cursor = connection.cursor()
        sql = 'SELECT * FROM client ORDER BY id'
        cursor.execute(sql)
        clients = cursor.fetchall()
        return clients
    except ConnectionError as error:
        print(f'Ошибка при подключении или чтении из таблицы client: {error}')
        logger.error(f'Ошибка при подключении или чтении из таблицы client: {error}')


def form_request_to_ecom_api(account_id=test_account_id, warehouse_id=test_warehouse_id, products=None):
    data = {'data':
                {
                    'account_id': account_id,
                    'warehouse_id': warehouse_id,
                    'products': products
                }
            }
    return data


def call_ecom_api(url, data):  # вызов метода /stocks
    response = requests.post(url, json=data)
    if response.status_code == 200:
        logger.info('Остатки успешно записаны.')
    else:
        logger.error(f'Ошибка при обработке запроса в API stocks. Статус код: {response.status_code}.')
    return response.json()


class Connector:

    def __init__(self, url):
        self.url = url  # URL сайта или API поставщика откуда загружается информация о товарах


class FTPConnector(Connector):  # --- ДОРАБОТАТЬ ---

    def __init__(self, user, password, port, timeout, resource_url):
        super().__init__(resource_url)
        self.user = user
        self.password = password
        self.port = port
        self.timeout = timeout

    def connect_to_ftp(self):
        ftp_obj = FTP(self.url)
        ftp_obj.connect(host=self.url, port=self.port, timeout=self.timeout)
        ftp_obj.login(user=self.user, passwd=self.password)
        return ftp_obj

    def get_data_from_ftp(self):  # доработать функцию
        ftp_obj = self.connect_to_ftp()
        file = open('data.xls', 'wb')
        ftp_obj.retrbinary('data.xls', file.write, 1024)
        df = pd.read_excel('data.xls', sheet_name='Sheet1')
        data = [tuple(x) for x in df.to_numpy()]
        file.close()
        ftp_obj.quit()
        return data

    def send_stocks_to_ecom_api(self):
        products = self.get_data_from_ftp()  # получить список товаров поставщика
        data = form_request_to_ecom_api(products=products)
        return call_ecom_api(STOCKS_API_URL, data)  # вызвать API метод /stocks


class ExcelFileConnector(Connector):

    def __init__(self, sheet_name, fields_mapping, resource_url):
        super().__init__(resource_url)
        self.sheet_name = sheet_name  # название листа в Excel файле
        self.fields_mapping = fields_mapping  # маппинг полей

    def map_data(self, df):  # маппинг полей: API stocks ---> Эксель файл поставщика
        df = df[[self.fields_mapping['offer_id'], self.fields_mapping['stock']]]
        mapped_data = [{'offer_id': row[0], 'stock': row[1]} for i, row in df.iterrows()]
        return mapped_data

    def get_data_from_excel_file(self):
        df = pd.read_excel(self.url, sheet_name=self.sheet_name)  # создание объекта DataFrame
        return self.map_data(df)

    def send_stocks_to_ecom_api(self):
        products = self.get_data_from_excel_file()  # получить список товаров поставщика
        data = form_request_to_ecom_api(products=products)
        return call_ecom_api(STOCKS_API_URL, data)  # вызвать API метод /stocks


class APIConnector(Connector):

    def __init__(self, api_key, supplier_func, resource_url):
        super().__init__(resource_url)
        self.api_key = api_key  # API-ключ
        self.supplier_func = supplier_func  # имя функции, которая вызывается для обращения в API поставщика

    def get_stocks_from_supplier_api(self):
        supplier_func = self.supplier_func
        func_name = globals()[supplier_func]
        products = func_name(self.url, self.api_key)  # список товар-остаток [{'offer_id': 'abc', 'stock': 100} ... ]
        return products

    def send_stocks_to_ecom_api(self):
        products = self.get_stocks_from_supplier_api()  # получить список товаров поставщика
        data = form_request_to_ecom_api(products=products)
        return call_ecom_api(STOCKS_API_URL, data)  # вызвать API метод /stocks


def main():
    clients = show_client_list()
    print('Список клиентов:')
    print('Client ID / Компания / ...')
    [print(client) for client in clients]
    client_id = input('\nВведите Client ID: ')

    suppliers = read_from_suppliers_db(client_id)  # подсоединиться к БД и получить список поставщиков (список словарей)

    print('Список поставщиков для обновления остатков:')
    if suppliers:
        print(list(dict(suppliers[0]).keys()))
        [print(list(dict(supplier).values())) for supplier in suppliers]
    input('\nНажмите любую клавишу для продолжения')

    # цикл по поставщикам (ЗАПУСТИТЬ В РАЗНЫХ ПОТОКАХ)
    for supplier in suppliers:  # только те поставщики, где last_request_date < current_date
        supplier = dict(supplier)
        supplier_id = supplier['id']
        company = supplier['company']
        resource_url = supplier['resource_url']
        connection_method = supplier['connection_method']

        connector = None
        if connection_method == 'api':
            api_key = supplier['api_key']
            supplier_func = supplier['supplier_func']
            connector = APIConnector(api_key, supplier_func, resource_url)
        elif connection_method == 'excel':
            fields_mapping = supplier['fields_mapping']
            connector = ExcelFileConnector('Sheet1', fields_mapping, resource_url)
        elif connection_method == 'ftp':
            user = supplier['user']
            password = supplier['password.txt']
            connector = FTPConnector(user, password, 80, 10, resource_url)  # port=80, timeout=10

        response = connector.send_stocks_to_ecom_api()
        update_last_request_date(client_id, supplier_id)  # записываем в таблицу suppliers текущую дату
        print(f'Обработка остатков поставщика {company} завершена.', response)

    print('Работа программы завершена.')


if __name__ == '__main__':
    main()
