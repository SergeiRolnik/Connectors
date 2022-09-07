from flask import Flask, request, abort
from flask_restful import Api, Resource, reqparse
from db import run_sql
import datetime
from config import *

app = Flask(__name__)
api = Api(app)
parser = reqparse.RequestParser()
parser.add_argument("data", type=dict, location="json", required=True)


class AddStocksToDB(Resource):
    def post(self):
        args = parser.parse_args()
        data = args['data']
        account_id = data['account_id']
        warehouse_id = str(data['warehouse_id'])
        products = data['products']
        total_num_of_products = len(products)

        # валидация вводимых клиентом данных
        if total_num_of_products > MAX_NUMBER_OF_PRODUCTS:
            abort(400, f'В запросе более {MAX_NUMBER_OF_PRODUCTS} товаров.  Уменьшите кол-во товаров.')

        date_now = str(datetime.date.today())
        # преобразовать данные c запроса клиента в список кортежей [( ....), (.....), ....] (удобно для записи в БД)
        products = [tuple(product.values()) + (account_id, warehouse_id, date_now) for product in products]

        # записать остатки в таблицу stock_by_wh (переписать код, чтобы добавлять сразу все записи в одном sql запросе)
        for product in products:
            sql = 'INSERT INTO stock_by_wh (offer_id, fbo_present, account_id, warehouse_id, date) ' \
                  'VALUES (%s, %s, %s, %s, %s) RETURNING id'
            result = run_sql(sql, product)  # добавлено RETURNING id, чтобы не вылезала ошибка в функции run_sql

        return {'message': f'В базу данных добавлено {len(products)} товаров'}


api.add_resource(AddStocksToDB, '/stocks')
app.run(debug=True)
