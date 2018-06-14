# coding:utf-8
import os

import math
import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Table, create_engine
from sqlalchemy.orm import sessionmaker, mapper, lazyload, joinedload

# def look_up_product():
#     user_input=raw_input('please enter the product barcode '
#                          'that you wish to checkin to the fridge: \n')
#     for prod in session.query(Product).filter(Product.product == user_input):
#         print prod.product
#         # Here's the nicety: to update just change the object directly:
#         prod.ammount = prod.ammount + 1
#     session.flush()
#     session.commit()
# Prepare high-level objects:
# class Product(object): pass
# engine = sa.create_engine('mysql://root:$$@localhost/fillmyfridge')
# session = sa.orm.create_session(bind=engine)
# product_table = sa.Table('products', sa.MetaData(), autoload=True)
# sqlalchemy.orm.mapper(Product, product_table)
# # 子查询
# stmt = session.query(Address.user_id, func.count('*').label("address_count")).group_by(Address.user_id).subquery()
# print(session.query(User, stmt.c.address_count).outerjoin((stmt, User.id == stmt.c.user_id)).order_by(User.id).all())
#
# # exists
# print(session.query(User).filter(exists().where(Address.user_id == User.id)))
# print(session.query(User).filter(User.addresses.any()))
# 字符串匹配
# MyModel.query.filter(sqlalchemy.not_(Mymodel.name.contains('a_string')))
# DBSession().query(user).filter(user.u_name.like('%三%')).filter(user.u_name.like('%猫%'))
# DBSession().query(user).filter(and_(user.u_name.like('%三%'), user.u_name.like('%猫%')))

def get_orders(tablename='orders', connection_str = "sqlite:///orders.sqlite", date='20180302', client_id="88998016"):
    engine = create_engine(connection_str)
    connection = engine.connect()
    meta = MetaData(bind=engine)
    table = Table(tablename.lower(), meta, autoload=True)
    Session = sessionmaker(bind=engine)
    session = Session()
    class Order(object):
        pass
    # Order.Entry.amount.desc()
    sqlalchemy.orm.mapper(Order, table)
    fetcher = session.query(table)\
        .filter(table.c.Date <= int(date))\
        .filter(table.c.AccountCode == int(client_id))\
        .order_by(table.c.Date.desc())
    _order = session.query(Order)
    from sqlalchemy import desc
    _order = _order.order_by(desc(Order.Date))
    # _order = _order.order_by(Order.Date.desc())
    _order = _order.all()
    res = fetcher.all()
    # result_dict = [u.__dict__ for u in res]
    # ow = dict(zip(row.keys(), row))
    temp_list = [r._asdict() for r in res]
    # pd.DataFrame.from_dict()
    if not temp_list:
        print("no history data")
        return
    import pandas
    df = pandas.DataFrame.from_records(temp_list, exclude=['id'])
    df = df.drop_duplicates()
    print(df)
    excel_file_name = './HistoryStatement_{client_id}_{date}.xlsx'.format(client_id=client_id, date=date)
    writer = pd.ExcelWriter(excel_file_name, engine='xlsxwriter')
    cols_seq = ['Contract', 'LongPosit', 'BidPrice', 'ShortPosit', 'AskPrice',
                'Previous_SP', 'Settlement_price', 'Position_P/L', 'Margin', 'H/S', 'AccountCode'
                ]
    cols_seq_order = ['Date', 'Exchange', 'Contract', 'Serial_No.', 'Buy/Sell', 'H/S',
                      'Trade_Price', 'Lots', 'Value', 'Open/Close', 'Commission', 'P/L1',
                      'P/L2', 'AccountCode',
                      ]
    df.to_excel(writer, sheet_name='Orders',
                      startrow=1, startcol=0,
                      # header=False,
                      columns=cols_seq_order,
                      index=False
                      )
    workbook = writer.book

    # Add a header format.
    header_format = workbook.add_format({
        'bold': True,
        'text_wrap': False,

        'border': 1})
    header_format.set_align('center')
    header_format.set_align('vcenter')
    header_format.set_border(1)
    header_format.set_font_size(12)

    worksheet_order = writer.sheets['Orders']
    worksheet_order.write(0, 0, "Order Details", header_format)

    writer.save()
    # temp_list = []

    # def row2dict(row):
    #     d = {}
    #     for column in row._fields:
    #         d[str(column)] = row[column]
    return

def get_vol(date, acc_id, future_id, tablename='trades'):
    engine = create_engine('sqlite:///trades.sqlite')
    connection = engine.connect()
    # meta = MetaData(bind=engine, reflect=True)
    # o_table = meta.tables[tablename.lower()]
    meta = MetaData(bind=engine)
    table = Table(tablename.lower(), meta, autoload=True)
    Session = sessionmaker(bind=engine)
    session = Session()

    class MyTrade(object):
        pass

    sqlalchemy.orm.mapper(MyTrade, table)
    # for a_tr in session.query(MyTrade).filter(table.c.date == date).filter(table.c.future_id == future_id):
    #     # print(a_tr)
    #     a_tr.turnover = -1
    # session.flush()
    # session.commit()
    # Cookie).filter(Cookie.cookie_name == 'chocolate chip').first
    fetcher = session.query(table).filter(table.c.acc_id == acc_id)\
        .filter(table.c.future_id == future_id)\
        .filter(table.c.date == date)\
        .filter(table.c.salesman != '公共虚拟人员')
    # fetcher = session.query(table).filter_by(acc_id = acc_id)

    res = fetcher.first()
    if not res:
        fetcher = session.query(table).filter(table.c.acc_id == acc_id) \
            .filter(table.c.future_id == future_id) \
            .filter(table.c.date == date)
        res = fetcher.first()
    vol = 0
    if res:
        vol = res.trading_volume
    else:
        vol = 0
    return vol


def main():
    get_orders()
    # # 59401	3063700011	20171215	任长庆	2	113130	白银	青岛黄岛区长江中路证券营业部	吕娟娟
    # date = 20171215
    # acc_id = 3063700011
    # future_id = '白银'
    # get_vol(date, acc_id, future_id)
    return


if __name__ == '__main__':
    main()
