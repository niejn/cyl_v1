import pandas as pd
import os
import math
import io
import re
import xlsxwriter
from src.db.db_get import get_orders

class MyException(Exception):
    def __init__(self,message):
        Exception.__init__(self)
        self.message=message

def readall(path):
    files = os.listdir(path)
    excel_files = []
    for file in files:
        file = path + '/' + file
        if not os.path.isfile(file):
            continue
        if file.endswith('txt'):
            excel_files.append(file)

    return excel_files
def normalize_df(client_id=None, pos_dict=None, temp_df=None):
    temp_df = temp_df.reset_index(drop=True)
    print(temp_df)
    temp_index = temp_df.index[temp_df[0].str.contains('---')].tolist()
    temp_df = temp_df.drop(temp_df[temp_df[0].str.contains('---')].index)
    print(temp_df)
    test = temp_df[0].str[1:-1]
    f_pos_df = temp_df[0].str[1:-1].str.split('|', expand=True)
    f_pos_df = f_pos_df.fillna("")
    f_pos_df = f_pos_df.applymap(lambda x: x.strip())
    f_pos_df = f_pos_df.reset_index(drop=True)
    print(f_pos_df)
    f_pos_df.columns = f_pos_df.iloc[0]
    f_pos_df = f_pos_df.reindex(f_pos_df.index.drop(0))
    print(f_pos_df)


    # pos_dict = {'Instrument': 'Contract', 'Long Pos.': 'LongPosit', 'Avg Buy Price': 'BidPrice',
    #             'Short Pos.': 'ShortPosit', 'Avg Sell Price': 'AskPrice',
    #             'Prev. Sttl': 'Previous_SP', 'Sttl Today': 'Settlement_price',
    #             'Accum. P/L': 'Position_P/L', 'Margin Occupied': 'Margin', 'S/H': 'H/S',
    #             }
    names_list = list(pos_dict.keys())
    f_pos_df = f_pos_df[names_list]
    print(f_pos_df)
    f_pos_df.rename(columns=pos_dict, inplace=True)
    print(f_pos_df)
    f_pos_df = f_pos_df[:-1]
    f_pos_df.loc[:, 'AccountCode'] = client_id
    return f_pos_df

def get_data_from_ctp(file_path = './20180302-88998016TBT_English.txt', rule_dict=None):
    # 统计  Lots， Value，Commission列，其他列不统计，第一列填入Sum_, 其他列并填入N/A
    # file_path = './20180302-88998016TBT_English.txt'
    # df = pd.read_table(file_path, sep='|', names=range(14),header=None,encoding='gb2312',)
    df = pd.read_table(file_path, names=range(1),header=None,encoding='gb2312',)
    # print( movies_data.head())
    df[0] = df[0].str.strip()
    # print(df[0]) Deposit/Withdrawal
    form_header = ['Settlement Statement(Trade-for-Trade)', 'Account Summary Currency:CNY', 'Deposit/Withdrawal'
                   'Warrant Pledge',  'Transaction Record',   'Delivery', 'Position Closed',  'Positions',]
    temp_index = df.index[df[0].isin(form_header) ].tolist()
    temp_index = sorted(temp_index)
    header_index_dict = {df[0][a_index]:a_index for a_index in temp_index}
    last_line = len(df[0])
    end_index = temp_index[1:]
    end_index.append(last_line)
    header_end_index_dict = {df[0][a_index]:end_index for a_index, end_index  in zip(temp_index, end_index)}
    # for a_index in temp_index:
    #     print(df[0][a_index])
    # [2, 5, 21, 28, 40, 48]
    print(temp_index)
    settle_index = header_index_dict['Settlement Statement(Trade-for-Trade)']
    settle_end_index = header_end_index_dict['Settlement Statement(Trade-for-Trade)']
    account_index = temp_index[1]
    settle_df = df[settle_index+1: settle_end_index]
    print(settle_df)
    settle_str = settle_df.values.tolist()
    phanzi = re.compile(u'[\u4e00-\u9fa5]+');
    nums_list = []
    for astr in settle_str:
        line = astr[0]
        res = phanzi.findall(line)
        nums = re.findall(r'([a-zA-Z]*\d+)', line)
        if nums:
            nums_list.append(nums)
    if len(nums_list) < 2:
        raise Exception("Settlement Statement 数据出问题")
        return
    client_id = nums_list[0][0]
    report_date = nums_list[1][0]

    transaction_dict = {'Date':'Date',	'Exchange':'Exchange',	'Instrument':'Contract',
                        'Trans.No.':'Serial_No.',	'B/S':'Buy/Sell',	'S/H':'H/S',
                        'Price':'Trade_Price',	'Lots':'Lots',	'Turnover':'Value',
                        'O/C':'Open/Close',	'Fee':'Commission',	'Total  P/L':'P/L1',
                        'Premium Received/Paid':'P/L2',}
    # Transaction_index = temp_index[3]
    if 'Transaction Record' in header_index_dict:
        Transaction_index = header_index_dict['Transaction Record']
        Transaction_end_index = header_end_index_dict['Transaction Record']
    # Position_Closed_index = temp_index[4]
        transaction_df = df[Transaction_index+1: Transaction_end_index]
    # print(transaction_df)
        f_transaction_df = normalize_df(client_id=client_id, pos_dict=transaction_dict, temp_df= transaction_df)
    # f_transaction_df[['Lots', 'Value','Commission',]] = f_transaction_df[['Lots', 'Value','Commission',]].astype(float)
    # f_transaction_df.at[df.index[-1], 'Lots'] = f_transaction_df['Lots'].sum()
    # image_name_data['id'] = image_name_data['id'].map('{:.0f}'.format)
        new_row = ["" for i in range(len(f_transaction_df.columns))]
        pd.Series(new_row, index=f_transaction_df.columns)
    # f_transaction_df = f_transaction_df.append(pd.Series(new_row, index=f_transaction_df.columns), ignore_index=True)
        f_transaction_df[['Lots', 'Value', 'Commission', ]] = \
            f_transaction_df[['Lots', 'Value', 'Commission', ]].astype(float)
        f_transaction_df = f_transaction_df.append(f_transaction_df.sum(numeric_only=True), ignore_index=True)
        f_transaction_df.at[f_transaction_df.index[-1], 'Date'] = 'Sum_'
    # f_transaction_df.at[5, 'Lots'] = f_transaction_df['Lots'].sum()
    # f_transaction_df.at[5, 'Value'] = f_transaction_df['Value'].sum()
    # f_transaction_df.at[5, 'Commission'] = f_transaction_df['Commission'].sum()
    # new_row.at[5, 'Lots'] = f_transaction_df['Lots'].sum()
    # new_row.at[5, 'Value'] = f_transaction_df['Value'].sum()
    # new_row.at[5, 'Commission'] = f_transaction_df['Commission'].sum()
        print(f_transaction_df)
    else:
        f_transaction_df = None
    '''
     str =line.split()
            phanzi=re.compile(u'[\u4e00-\u9fa5]+');

            res = phanzi.findall(line)
            nums = re.findall(r'([a-zA-Z]*\d+)', line)
            '''
    if 'Positions' in header_index_dict:
        pos_index = temp_index[-1]
        positions_df = df[pos_index + 1:]
        positions_df = positions_df.reset_index(drop=True)
        print(positions_df)
        temp_index = positions_df.index[positions_df[0].str.contains('---')].tolist()
        positions_df = positions_df.drop(positions_df[positions_df[0].str.contains('---')].index)
        print(positions_df)
        f_pos_df = positions_df[0].str[1:-1].str.split('|', expand=True).applymap(lambda x: x.strip())
        f_pos_df = f_pos_df.reset_index(drop=True)
        print(f_pos_df)
        f_pos_df.columns = f_pos_df.iloc[0]
        f_pos_df = f_pos_df.reindex(f_pos_df.index.drop(0))
        print(f_pos_df)

        pos_dict = {'Instrument': 'Contract', 'Long Pos.': 'LongPosit', 'Avg Buy Price': 'BidPrice',
               'Short Pos.': 'ShortPosit', 'Avg Sell Price': 'AskPrice',
               'Prev. Sttl': 'Previous_SP', 'Sttl Today': 'Settlement_price',
               'Accum. P/L': 'Position_P/L', 'Margin Occupied': 'Margin', 'S/H': 'H/S',
                    'Exchange ID': 'Exchange',
               }
        f_pos_df = f_pos_df[:-1]
        # add product map to exchange
        if rule_dict:
            print(f_pos_df)
            print(f_pos_df['Product'])
            f_pos_df['Exchange ID'] = f_pos_df.apply(lambda x: rule_dict[x.Product.upper()]['Exchange ID'], axis=1)
            print(f_pos_df['Exchange ID'] )
        names_list = list(pos_dict.keys())
        f_pos_df = f_pos_df[names_list]
        print(f_pos_df)
        f_pos_df.rename(columns=pos_dict,inplace=True)
        print(f_pos_df)
        # f_pos_df = f_pos_df[:-1]
        f_pos_df.loc[:, 'AccountCode'] = client_id

    excel_file_name = './DailyStatement_{client_id}_{date}.xlsx'.format(client_id=client_id, date=report_date)
    writer = pd.ExcelWriter(excel_file_name, engine='xlsxwriter')
    cols_seq = ['Exchange', 'Contract',	'LongPosit',	'BidPrice',	'ShortPosit',	'AskPrice',
                'Previous_SP',	'Settlement_price',	'Position_P/L',	'Margin',	'H/S', 'AccountCode'
    ]
    cols_seq_order = ['Date',	'Exchange',	'Contract',	'Serial_No.',	'Buy/Sell',	'H/S',
                      'Trade_Price',	'Lots',	'Value',	'Open/Close',	'Commission',	'P/L1',
                      'P/L2',	'AccountCode',
    ]
    # Convert the dataframe to an XlsxWriter Excel object.
    if f_transaction_df is not None:
        f_transaction_df.to_excel(writer, sheet_name='Orders',
                          startrow=1, startcol=0,
                          # header=False,
                          columns=cols_seq_order,
                          index=False
                          )
    if f_pos_df is not None:

        f_pos_df.to_excel(writer, sheet_name='Position',
                          startrow=1, startcol=0,
                          # header=False,
                          columns=cols_seq,
                          index=False
                          )

    # Close the Pandas Excel writer and output the Excel file.
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
    if 'Position' in writer.sheets:
        worksheet = writer.sheets['Position']
        worksheet.write(0, 0, "Positions_P/L Details", header_format)
    if 'Orders' in writer.sheets:
        worksheet_order = writer.sheets['Orders']
        worksheet_order.write(0, 0, "Order Details", header_format)

    writer.save()
    # DailyStatement_99801601_20170808
    from src.db.db_insert import insert_direct
    if f_transaction_df is not None:
        print(f_transaction_df)
        f_transaction_df = f_transaction_df[f_transaction_df['Date'] != 'Sum_']
        print(f_transaction_df)
        data_list = f_transaction_df.to_dict(orient='records')
        insert_direct(data_list, tablename='orders')
    return client_id, report_date


def gen_history_orders():

    return

def get_trade_rule(config_path='./config/常规保证金及基本交易规则.csv'):
    df = pd.read_csv(config_path, header=0, encoding='gbk', index_col=2)
    df_ori_cols = df.columns.tolist()
    print(df_ori_cols)

    df = df[['交易所', '品种', '合约乘数', '最小报价', 'LAST_TRADE_DATE']]
    print(df)

    df_header = {'交易所': 'Exchange ID', '品种': 'CHI_CODE', '合约乘数': 'LOT SIZE/MULTILPER', '最小报价': 'TICK_VALUE',
                 '代码': 'GCS_CMD_CODE'}
    df.rename(columns=df_header, inplace=True)
    print(df)

    tdict = df.to_dict('index')
    print(tdict)
    up_dict = {key.upper(): val for key, val in tdict.items()}

    return up_dict



def main():
    rule_dict = get_trade_rule()
    # get_trade_rule()
    # df['Exchange ID'] = df.apply(
    #     lambda x: rule_dict[x.GCS_CMD_CODE.upper()]['Exchange ID'], axis=1)
    files = readall("./txt")
    for afile in files:
        client_id, report_date = get_data_from_ctp(afile, rule_dict)
        get_orders(connection_str="sqlite:///src/db/orders.sqlite", client_id=client_id, date=report_date)
    #   sqlite:///src/db/orders.sqlite
    # get_orders(connection_str="sqlite:///src/db/orders.sqlite", )

    return
if __name__ == '__main__':
    main()











