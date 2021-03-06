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
    Column('Date', Integer()),
    Column('Exchange', String(50), index=True),
    Column('Contract', String(50)),
    Column('Serial_No.', Integer()),
    Column('Buy/Sell', String(50)),
    Column('H/S', String(50)),
    Column('Trade_Price', Float(10, 2)),
    Column('Lots', Integer()),
    Column('Value', Integer()),
    Column('Open/Close', String(50)),
    Column('Commission', Float(10, 2)),
    Column('P/L1', Float(10, 2)),
    Column('P/L2', Float(10, 2)),
    Column('AccountCode', Integer()),
)

# Account	Name	Trading Volume		increase	growth_rate	Corporate_trade_vol	Corporate occupation ratio	market_trade_vol	marketoccupation ratio
# Base.metadata.create_all()
ans = metadata.create_all(engine)
print(ans)