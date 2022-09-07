# максимальное кол-во товаров можно отправлять в одном запросе
MAX_NUMBER_OF_PRODUCTS = 1000

# данные для подключения к локальной БД (таблицы client, suppliers, supplier_client)
LOCAL_DB_DSN = f'postgresql://postgres:postgres@localhost/postgres'

# URL метода для записи остатков
STOCKS_API_URL = 'http://localhost:5000/stocks'

# данные для подключения к БД Ecom Seller (market_db)
MARKET_DB_DSN = """
host = rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net
port = 6432
dbname = market_db
user = srolnik
password = your_password_here
target_session_attrs = read-write
sslmode = verify-full
sslrootcert=root.crt
"""
