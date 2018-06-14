import os

import math
import pandas as pd
from sqlalchemy import MetaData, Table, create_engine
# metadata = MetaData()
# engine = create_engine('sqlite:///trades.sqlite')
# trades = Table('trades', metadata, autoload=True, autoload_with=engine)
#
# ins = trades.insert()
# # result = connection.execute(ins, inventory_list)
# connection = engine.connect()
from sqlalchemy.exc import IntegrityError


def readAll(path, fileType):
    files = os.listdir(path)
    # textContainer = []
    excelFileList = []
    for file in files:
        file = path + '/' + file
        if not os.path.isfile(file):
            continue
        if file.endswith(fileType):
            excelFileList.append(file)
            print(file)

    return excelFileList

# 日期格式: 2017-12-13
def get_time_futureid_v1(fn=None):
    # fn = '交易统计信息_2017-12-11-5年期国债.xml'
    ans = fn.split('-', 1)
    pre_ans = ans[0]
    tail_ans = ans[1]

    ftime = tail_ans.split('.')[0]
    ftime = ftime.replace('-', '')

    return ftime


def get_time_futureid(fn=None):
    # fn = '交易统计信息_2017-12-11-5年期国债.xml'
    ans = fn.rsplit('-', 1)
    pre_ans = ans[0]
    tail_ans = ans[1]
    pre_split = pre_ans.split('_')
    tail_split = tail_ans.split('.')
    ftime = pre_split[1]
    ftime = ftime.replace('-', '')
    future_id = tail_split[0]
    return (ftime, future_id)


def get_data(futures_path = 'xls/futures', file_type = 'xlsx'):
    # futures_path = 'xls/futures'
    fut_files = readAll(futures_path, file_type)
    df_list = []
    for a_file in fut_files:
        ftime = get_time_futureid_v1(a_file)
        # df = pd.read_csv(a_file, header=0, skipfooter=1, encoding='python', encoding='utf8')
        # pandas的索引函数主要有三种：
        # loc 标签索引，行和列的名称
        # iloc 整型索引（绝对位置索引），绝对意义上的几行几列，起始索引为0
        # ix 是 iloc 和 loc的合体
        # at是loc的快捷方式
        # iat是iloc的快捷方式

        df = pd.read_excel(a_file, header=0, skip_blank_lines=True)

        df[['成交手数', '成交金额']] = df.groupby(['资金帐号', '合约品种'])["成交手数", '成交金额'].transform('sum')
        df = df.groupby(['资金帐号', '合约品种'], as_index=False).first().reset_index()

        df.rename(columns=lambda x: x.strip(), inplace=True)
        remain_cols = ['营业部', '业务员', '客户姓名', '客户号', '合约品种', '成交手数', '成交金额']
        df = df[remain_cols]
        print(df.columns.tolist())
        print(df)

        df = df.fillna('0')
        # df = df.loc[lambda df: df.交易日 == '0']
        # print(df)axis=1
        criterion = df['营业部'].map(lambda x: not(x.startswith('合计')))
        print(criterion)
        # se_df = df[df.营业部 != '合计:']
        df = df[criterion]
        # print(df[criterion])
        # df = df.drop(criterion)
        # print(df)
        # df = df.drop(df.index[-1])
        print(df)
        # print(df.columns.tolist())

        cols_dict = {'营业部':'department', '业务员':'salesman', '客户姓名':'acc_name',
                     '客户号':'acc_id', '合约品种':'future_id', '成交手数':'trading_volume',
                     '成交金额':'turnover', '日期':'date'}
        df.rename(columns=cols_dict, inplace=True)
        df['date'] = ftime

        # cols = ['acc_id', 'date', 'acc_name', 'trading_volume', 'future_id']
        df_list.append(df)
    return df_list


def get_data_v2():
    futures_path = 'xls/futures'
    fut_files = readAll(futures_path, 'csv')
    df_list = []
    for a_file in fut_files:
        ftime, future_id = get_time_futureid(a_file)
        # df = pd.read_csv(a_file, header=0, skipfooter=1, encoding='python', encoding='utf8')
        # pandas的索引函数主要有三种：
        # loc 标签索引，行和列的名称
        # iloc 整型索引（绝对位置索引），绝对意义上的几行几列，起始索引为0
        # ix 是 iloc 和 loc的合体
        # at是loc的快捷方式
        # iat是iloc的快捷方式

        df = pd.read_csv(a_file, header=0, skip_blank_lines=True, encoding='utf-8')
        # df['date'] = ftime
        # df['future_id'] = future_id
        # print(df[0])
        # df.filter(like='bbi', axis=0)
        # arg in col == True
        # test = None
        # df = df.filter(items=['投资者名称'], like='arg in 投资者名称 != None', axis=0)
        # last_row = df.iloc[-1:]
        # last_row = df.tail(1)
        # last_row = df[df['投资者代码'] == float('nan')]
        # last_row = df.loc[df.loc[:, "投资者代码"] != None, :]
        # last_row = df.loc[-3:-1,:]
        # print(last_row)
        # math.isnan(df['投资者名称'])
        last_row  = df.tail(1)
        print(last_row.投资者名称)
        t_unit = last_row.投资者名称
        print(math.isnan(t_unit))
        # df = df.where(df)
        # df.loc[lambda df: math.isnan(df.投资者名称), :]
        # df =  df.投资者名称.loc[lambda s: len(s) > 0]
        # print(df)
        # df = df.loc[lambda df: len(df.投资者名称) > 0]

        # criterion = df2['a'].map(lambda x: x.startswith('t'))
        df = df.fillna('0')
        df = df.loc[lambda df: df.交易日 == '0']
        print(df)
        criterion = df['交易日'].map(lambda x: x.startswith('总计'))
        print(criterion)
        print(df[criterion])
        df = df.drop(df[criterion])
        print(df)
        df = df.drop(df.index[-1])
        print(df)
        print(df.columns.tolist())
        df.rename(columns=lambda x: x.strip(), inplace=True)
        remain_cols = ['投资者代码', '投资者名称', '总成交量']
        df = df[remain_cols]
        # df.rename(columns=df_header, inplace=True)
        # 投资者代码	投资者名称 总成交量

        cols_dict = {'投资者代码': 'acc_id', '投资者名称': 'acc_name', '总成交量': 'trading_volume'}
        df.rename(columns=cols_dict, inplace=True)
        df['date'] = ftime
        df['future_id'] = future_id
        cols = ['acc_id', 'date', 'acc_name', 'trading_volume', 'future_id']
        df_list.append(df)
    return df_list



def insert_db(data, tablename='trades'):
    metadata = MetaData()
    engine = create_engine('sqlite:///orders.sqlite')
    connection = engine.connect()
    # transaction = connection.begin()

    try:

        data.to_sql(tablename.lower(), engine, if_exists='append', index=False)

        # transaction.commit()
        ans = True
    except IntegrityError as error:
        # transaction.rollback()
        print(error)
    except Exception as error:
        # transaction.rollback()
        print(error)
    finally:
        connection.close()
    return


def insert_direct(data_list=None, tablename='trades'):
    # metadata = MetaData()
    # src/db/orders.sqlite
    engine = create_engine('sqlite:///src/db/orders.sqlite')
    connection = engine.connect()
    # meta = MetaData(bind=engine, reflect=True)
    # o_table = meta.tables[tablename.lower()]
    meta = MetaData(bind=engine)
    o_table = Table(tablename.lower(), meta, autoload=True)
    # action = o_table.insert().values(data_list[0])
    transaction = connection.begin()
    ans = False
    try:

        # ans = connection.execute(action)
        # ins = o_table.insert().values(data_list)
        ins = o_table.insert()
        ans = connection.execute(ins, data_list)
        # for data in data_list:
        #     action = o_table.insert().values(data)
        #     ans = connection.execute(action)
        transaction.commit()
        ans = True
    except IntegrityError as error:
        transaction.rollback()
        print(error)
    except Exception as error:
        transaction.rollback()
        print(error)
    finally:
        connection.close()

    return ans


def pd_insert_db(df):
    data_list = df.to_dict(orient='records')
    insert_direct(data_list)

    return

def init_db(path='./insert_db'):
    df_list = get_data(futures_path=path)
    for df in df_list:
        pd_insert_db(df)
    # df_list = get_data()
    # for df in df_list:
    #     pd_insert_db(df)
    return
def main():
    init_db()
    # data.to_sql(tablename.lower(), engine, if_exists='append', index=False)
    # df_list = get_data()
    # df = df_list[0]
    # print(df)
    # for df in df_list:
    #     pd_insert_db(df)

    return


if __name__ == '__main__':
    main()
