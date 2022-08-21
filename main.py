import requests
from ftplib import FTP
import psycopg2
import psycopg2.extras
import pandas as pd


STOCKS_DB_DSN = 'postgresql://postgres:postgres@localhost/postgres'
SUPPLIERS_DB_DSN = 'postgresql://postgres:postgres@localhost/postgres'

# название таблицы в БД
table_name_stocks = 'stock_by_wh'
table_name_suppliers = 'suppliers'

# столбцы БД товаров (таблица stock_by_wh), в которые будет добавлены данные от поставщика
# cols_stocks = ['fbo_present', 'offer_id', 'warehouse_id', 'date']
cols_stocks = ['fbo_present', 'offer_id', 'date']
cols_stocks = ','.join(cols_stocks)

def write_to_stocks_db(data):  # взять скачанные данные и записать их в БД stocks
    try:
        connection = psycopg2.connect(STOCKS_DB_DSN)
        cursor = connection.cursor()
        sql = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table_name_stocks, cols_stocks)
        cursor.executemany(sql, data)
        connection.commit()
        print('Данные о товарах успешно добавлены в БД (таблица stock_by_wh)')
    except ConnectionError as error:
        print('Ошибка при подключении или записи в БД', error)


def read_from_suppliers_db():  # подсоединиться к БД и получить список поставщиков
    try:
        connection = psycopg2.connect(SUPPLIERS_DB_DSN)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = 'SELECT * FROM %s' % table_name_suppliers  # возможно надо делать какую-то выборку из списка поставщиков
        cursor.execute(sql)
        suppliers = cursor.fetchall()
        return suppliers
    except ConnectionError as error:
        print('Ошибка при подключении или чтении из БД', error)


# общий класс соединения
class Connector:

    def __init__(self, url):
        self.url = url  # URL сайта или API поставщика откуда загружается информация о товарах


# класс для соединения по API
class APIConnector(Connector):

    def __init__(self, api_key, request_params, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = api_key  # API-ключ
        self.request_params = request_params  # словарь с параметрами API поставщика

    def get_headers(self):  # сформировать заголовки для запроса в API поставщика
        headers = {
            'Api-Key': self.api_key,
            'Content-Type': 'application/json'
        }
        return headers

    def get_response_from_api(self):
        response = requests.get(url=self.url, headers=self.get_headers(), json=self.request_params)
        if response.status_code == 200:
            return response.json()

    def convert_json_to_data(self, json_file):  # преобразовать json файл в список кортежей (для удобства записи в БД)
        pass

    def get_data(self):
        json_file = self.get_response_from_api()
        return self.convert_json_to_data(json_file)


# класс для загрузки файлов Excel
class ExcelFileConnector(Connector):

    def __init__(self, sheet_name, fields_mapping, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sheet_name = sheet_name  # название листа в Excel файле
        self.fields_mapping = fields_mapping  # маппинг полей

    def map_data(self, df):  # маппинг полей: Эксель файл поставщика ---> таблица stock_by_wh в БД
        df = df.to_dict()

        mapped_data = list(zip(
            df[self.fields_mapping['fbo_present']].values(),
            df[self.fields_mapping['offer_id']].values(),
            df[self.fields_mapping['date']].values()
        ))

        return mapped_data

    def get_data(self):
        df = pd.read_excel(self.url, sheet_name=self.sheet_name)  # создание объекта DataFrame
        return self.map_data(df)


# класс для соединения по FTP
class FTPConnector(Connector):

    def __init__(self, user, password, port, timeout, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user = user
        self.password = password
        self.port = port
        self.timeout = timeout

    def connect_to_ftp(self):
        ftp_obj = FTP(self.url)
        ftp_obj.connect(host=self.url, port=self.port, timeout=self.timeout)
        ftp_obj.login(user=self.user, passwd=self.password)
        return ftp_obj

    def get_data(self):
        ftp_obj = self.connect_to_ftp()
        file = open('data.xls', 'wb')
        ftp_obj.retrbinary('data.xls', file.write, 1024)
        # взять данные из файла data.xls
        df = pd.read_excel('data.xls', sheet_name='Sheet1')
        data = [tuple(x) for x in df.to_numpy()]
        file.close()
        ftp_obj.quit()
        return data


# подсоединиться к БД и получить список поставщиков
suppliers = read_from_suppliers_db()  # список словарей

# цикл по поставщикам и загрузка данных по товарам (---- ЗАПУСТИТЬ В РАЗНЫХ ПОТОКАХ!!! ----)
# возможно нужно отфильтровать поставщиков по какому-то параметру, например, last_request_date != datetime.today()
for supplier in suppliers:
    supplier = dict(supplier)
    company = supplier['company']
    resource_url = supplier['resource_url']
    connection_method = supplier['connection_method']
    user = supplier['user']
    password = supplier['password']
    api_key = supplier['api_key']
    last_request_date = supplier['last_request_date']
    request_params = {}

    # маппинг полей: таблица stock_by_wh в БД --> Excel файл (!!! ВЫНЕСТИ В ОТДЕЛЬНЫЙ JSON СТОЛБЕЦ В БД ПОСТАВЩИКОВ)
    fields_mapping = {
        'offer_id': 'offer_id',
        'fbo_present': 'stock',
        'date': 'updated_at'
    }

    if connection_method == 'api':
        connector = APIConnector(api_key, request_params, resource_url)
    elif connection_method == 'excel':
        connector = ExcelFileConnector('Sheet1', fields_mapping, resource_url)
    elif connection_method == 'ftp':
        connector = FTPConnector(user, password, 80, 10, resource_url)  # port=80, timeout=10

    print()
    print('Поставщик:', company)

    # получение данных от поставщика
    data = connector.get_data()
    print('Данные поставщика:', data)

    # запись информации о товарах в таблицу stock_by_wh
    write_to_stocks_db(data)

print('\nРабота программы завершена')