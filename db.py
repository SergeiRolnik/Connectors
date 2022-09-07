from psycopg2.pool import ThreadedConnectionPool
from config import MARKET_DB_DSN
from loguru import logger


# класс для работы с connection pooling
class ConnectionPool(ThreadedConnectionPool):
    def __init__(self, minconn, maxconn, *args, **kwargs):
        super().__init__(minconn, maxconn, *args, **kwargs)

    def connect_to_pool(self):
        return self.getconn()

    def disconnect_from_pool(self, connection):
        return self.putconn(connection)


# инициализируем пул соединений к БД master_db (макс. число соединений - кол-во передаваемых в правиле аккаунтов)
master_db_connection_pool = ConnectionPool(1, 100, MARKET_DB_DSN)


# ОДНА ФУНКЦИЯ ДЛЯ ВСЕХ ЗАПРОСОВ
def run_sql(sql: str, values: tuple):
    try:
        connection = master_db_connection_pool.connect_to_pool()
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(sql, values)
        result = cursor.fetchall()
        return result
    except Exception as error:
        logger.error(f'Ошибка {error} при обработке SQL запроса {sql}')
    finally:
        if connection:
            master_db_connection_pool.disconnect_from_pool(connection)
