import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import openpyxl
import xlsxwriter
import datetime

# 读取cider库数据
engine = create_engine('mysql+pymysql://leiming:vg4wHTnJlbWK8SY@101.200.97.76:3306/cider')
data_refund = pd.read_sql_table('shopify_refund', engine)

# 退货状态分布
data_refund_state = data_refund.loc[:, ['refund_date', 'status', 'refund_num', 'order_no','id']]
data_refund_state['refund_date'] = data_refund_state['refund_date'].dt.date
data_refund_state['pending_退货单数'] = np.where(data_refund_state['status'] == 'Pending', data_refund_state['order_no'], np.nan)
data_refund_state['pending_退货件数'] = np.where(data_refund_state['status'] == 'Pending', data_refund_state['refund_num'], 0)
data_refund_state['approved_退货单数'] = np.where(data_refund_state['status'] == 'Approved', data_refund_state['order_no'], np.nan)
data_refund_state['approved_退货件数'] = np.where(data_refund_state['status'] == 'Approved', data_refund_state['refund_num'], 0)
data_refund_state['resolved_退货单数'] = np.where(data_refund_state['status'] == 'Resolved', data_refund_state['order_no'], np.nan)
data_refund_state['resolved_退货件数'] = np.where(data_refund_state['status'] == 'Resolved', data_refund_state['refund_num'], 0)

a = data_refund_state.groupby(['refund_date'], as_index=False).agg({'pending_退货单数': ["nunique"], 'pending_退货件数': np.sum,
                                                   'approved_退货单数':["nunique"], 'approved_退货件数': np.sum, 'resolved_退货单数': ["nunique"],
                                                   'resolved_退货件数': np.sum})
a['合计_退货单数'] = a['pending_退货单数'] + a['approved_退货单数'] + a['resolved_退货单数']
a['合计_退货件数'] = a['pending_退货件数'] + a['approved_退货件数'] + a['resolved_退货件数']

# test : 退货状态分布
b = data_refund_state.groupby(['refund_date', 'status'], as_index=False).agg({'order_no': ["nunique"], 'refund_num': np.sum})
a.rename(columns={'refund_date': '退货日期'}, inplace = True)
a = a.set_index(['退货日期'])
a = a.replace(0, np.nan)


# 退货原因分布:
data_refund_reason = data_refund.loc[:, ['refund_reason', 'refund_num', 'order_no', 'id']]
data_refund_reason = data_refund_reason.groupby(['refund_reason'], as_index=False).agg({'order_no': ["nunique"], 'refund_num': np.sum})
data_refund_reason = data_refund_reason.sort_values(by = [('order_no', 'nunique'), ('refund_num','sum')], ascending = [False, False])
data_refund_reason.rename(columns={'order_no': '退货单数', 'refund_num': '退货件数', 'refund_reason' :'退货原因'}, inplace = True)
data_refund_reason = data_refund_reason.set_index(['退货原因'])

data_refund_reason.to_excel('/Users/edz/Documents/退货原因分布.xlsx')

# 退货按国家分布
data_refund_country = data_refund.loc[:, ['country', 'refund_num', 'order_no', 'id']]
data_refund_country = data_refund_country.groupby(['country'], as_index=False).agg({'order_no': ["nunique"], 'refund_num': np.sum})
data_refund_country = data_refund_country.sort_values(by = [('order_no', 'nunique'), ('refund_num','sum')], ascending = [False, False])
data_refund_country.rename(columns={'order_no': '退货单数', 'refund_num': '退货件数', 'country' :'国家'}, inplace = True)
data_refund_country = data_refund_country.set_index(['国家'])
data_refund_country.to_excel('/Users/edz/Documents/退货数据国家分布.xlsx')

# 按下单时间分布
data_refund_xx = data_refund.loc[:, ['order_date', 'refund_num', 'order_no', 'id']]
data_refund_xx['order_date'] = data_refund_xx['order_date'].dt.date
data_refund_xx = data_refund_xx.groupby(['order_date']).agg({'order_no': ["nunique"], 'refund_num': np.sum})

# 补全时间
all_days = pd.date_range(data_refund_xx.index.min(), data_refund_xx.index.max(), freq='D')
data_refund_xx = data_refund_xx.reindex(all_days)
data_refund_xx.rename(columns={'refund_num': '退货件数', 'order_date' :'下单时间'}, inplace = True)

# 读取总件数
xls = pd.read_excel('/Users/edz/Documents/退货分析.xlsx')
data_refund_xx['下单时间'] = data_refund_xx.index
data_refund_xx =  data_refund_xx.merge(xls, left_on = [('下单时间','')], right_on = ['下单时间'], how = 'left')
data_refund_xx = data_refund_xx.loc[:, [('下单时间', ''), ('order_no', 'nunique'), ('退货件数', 'sum') ,'总下单数', '退货率']]
data_refund_xx[('下单时间', '')] = data_refund_xx[('下单时间', '')].dt.date


# 按退货商品分布
data_refund_product = data_refund.loc[:, ['product_name', 'refund_num', 'order_no', 'id']]
data_refund_product = data_refund_product.groupby(['product_name'], as_index=False).agg({'order_no': ["nunique"], 'refund_num': np.sum})
data_refund_product.rename(columns={'order_no': '退货单数', 'refund_num': '退货件数', 'product_name' :'商品名称'}, inplace = True)
data_refund_product = data_refund_product.set_index(['商品名称'])





# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('/Users/edz/Documents/refund_analysis.xlsx', engine='xlsxwriter')

# Write each dataframe to a different worksheet.
a.to_excel(writer, sheet_name='退货状态分布')
data_refund_reason.to_excel(writer, sheet_name='退货原因分布')
data_refund_xx.to_excel(writer, sheet_name='退货下单时间分布')
data_refund_country.to_excel(writer, sheet_name='退货数据国家分布')
data_refund_product.to_excel(writer, sheet_name='退货商品分布')


# Close the Pandas Excel writer and output the Excel file.
writer.save()
