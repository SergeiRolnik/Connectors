import requests
from fake_useragent import UserAgent


def get_stocks_from_samson_api(url, api_key):
    ua = UserAgent()
    user_agent = ua.random

    headers = {
      'Accept': 'application/json',
      'User-Agent': user_agent,
      'Accept-Encoding': 'gzip'
    }

    params = {
      'api_key': api_key,
      'pagination_count': '2',
      'pagination_page': '1'
    }

    response = requests.get(url=url, params=params, headers=headers)
    products = response.json()['data']
    products = [{'offer_id': str(product['sku']), 'stock': product['stock_list'][3]['value']} for product in products]
    return products  # список товар-остаток [{'offer_id': 'abc', 'stock': 100} ... ]


def get_stocks_from_another_supplier_api(url, api_key):
    headers = {
        # здесь свой код
    }

    params = {
        'api_key': api_key
        # здесь свой код
    }

    response = requests.get(url=url, params=params, headers=headers)
    products = response.json()['data']  # заменить на свой код
    # здесь свой код
    return products
