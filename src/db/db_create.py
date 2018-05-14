#encoding:utf-8
from sqlalchemy import MetaData, Float
from datetime import datetime
from sqlalchemy import Table, Column, Integer, Numeric, String, ForeignKey, DateTime
from sqlalchemy import create_engine

# Column('date', Integer(), primary_key=True),
metadata = MetaData()
# engine = create_engine('sqlite:///:memory:')
engine = create_engine('sqlite:///orders.sqlite')

# 营业部 department
# 业务员 salesman
# 客户姓名	acc_name
# 客户号	acc_id
# 合约品种	future_id
# 成交手数 trading_volume
# 成交金额 turnover
# 日期 date
# Date	Exchange	Contract	Serial_No.	Buy/Sell	H/S	Trade_Price	Lots	Value	Open/Close	Commission	P/L1	P/L2	AccountCode


Trades = Table('orders', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('date', Integer()),
    Column('exchange', String(50), index=True),
    Column('contract', String(50)),
    Column('serial_no', Integer()),
    Column('buy_sell', String(50)),
    Column('h_s', String(50)),
    Column('trade_price', Float(10, 2)),
    Column('lots', Integer()),
    Column('value', Integer()),
    Column('open_close', String(50)),
    Column('commission', Float(10, 2)),
    Column('p_l1', Float(10, 2)),
    Column('p_l2', Float(10, 2)),
    Column('AccountCode', Integer()),
)

# Account	Name	Trading Volume		increase	growth_rate	Corporate_trade_vol	Corporate occupation ratio	market_trade_vol	marketoccupation ratio
# Base.metadata.create_all()
ans = metadata.create_all(engine)
print(ans)