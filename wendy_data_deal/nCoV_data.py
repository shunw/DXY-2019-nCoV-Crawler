import requests
import json

import MySQLdb as mysql
import re
import os
from datetime import datetime
import pytz

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import matplotlib.cm as cm
import matplotlib.colors as colors
from matplotlib import font_manager



# reference link: 
# http://cuihuan.net/wuhan/nCoV.html

def get_json_file(output_file): 
    '''
    output_file: the file name will be saved. 
    this is to get and save the json file for later usage
    '''
    response = requests.get('https://lab.isaaclin.cn/nCoV/api/area?latest=0')

    response.encoding = 'utf-8'

    json_data = json.loads(response.text)

    with open(output_file, 'w') as write_file: 
        json.dump(json_data, write_file)

def update_province_name(): 
    '''
    output_file: the file name will be saved. 
    this is to update all the province name list
    '''
    response = requests.get('https://lab.isaaclin.cn/nCoV/api/provinceName')

    response.encoding = 'utf-8'

    json_data = json.loads(response.text)

    with open('province_name.json', 'w', encoding = 'utf8') as write_file: 
        json.dump(json_data, write_file)

class Deal_nCoV_data(object): 
    def __init__(self, input_name): 
        self.input_name = input_name

    def prepare_df(self): 
        with open(self.input_name, 'r') as read_file: 
            raw_data = json.load(read_file)

        # print (raw_data['results'][0]['provinceName'], raw_data['results'][0]['country'], raw_data['results'][0]['cities'], raw_data['results'][0]['updateTime'])
        raw_data_ls = raw_data['results']
        country_dict = dict()
        for i in raw_data_ls: 
            temp_country = i['country']
            temp_provice = i['provinceName']
            if temp_country not in country_dict.keys(): 
                country_dict[temp_country] = [temp_provice]
            elif temp_provice in country_dict[temp_country]: continue
            else: 
                country_dict[temp_country].append(temp_provice)

        for k, v in country_dict.items(): 
            if len(v) == 1: continue
            else: 
                print (k, len(v))
        # print (raw_data['results'][2]['provinceName'], raw_data['results'][0]['country'], raw_data['results'][0]['cities'], raw_data['results'][0]['updateTime'])
        # print (len(raw_data['results']))
        # res_dict = todos['results']
        # print (len(res_dict[0]))

        # suc_dict = todos['success']
        # print (suc_dict)


        # print (res_dict['confirmedCount'])

    def final_run(self): 
        self.prepare_df()

def create_ncov_db(cursor): 
    # create ncov_data db
    sql = 'CREATE DATABASE IF NOT EXISTS ncov_data DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci'
    cursor.execute(sql)

cnt_id = 'country_id'
prv_id = 'province_id'
city_id = 'city_id'
cnt = 'country'
prv = 'provinceName'
city = 'cityName'
prv_sh = 'provinceShortName' # province short name
cfm_count = 'confirmedCount'
c_cfm_count = 'currentConfirmedCount'
spct_count = 'suspectedCount'
cured_count = 'curedCount'
dead_count = 'deadCount'
cmt = 'comment'
upt_time = 'updateTime'
dt_time = 'datetime'

def create_tbl_prov_data(cursor, data_stream = ''): 
    '''
    data_stream: is a list; default is empty
    to create and insert the province data in it
    '''
    
    tbl_name = 'prov_data'

    item_type_dict = {'string': [cnt, prv, prv_sh, cmt], 
        'float': [c_cfm_count, cfm_count, spct_count, cured_count, dead_count],
        'int': [cnt_id, prv_id],
        'date': [upt_time]
        }
    list_title = [prv_id, prv, prv_sh, cnt_id, cnt, c_cfm_count, cfm_count, spct_count, cured_count, dead_count, cmt, upt_time]

    # create table
    sql_title_dict = _get_title_dict(item_type_dict)
    sql_sentence = _create_tbl(tbl_name, sql_title_dict, list_title)
    cursor.execute(sql_sentence)

    # insert data
    if data_stream:
        # [prv, prv_sh, c_cfm_count, cfm_count, spct_count, cured_count, dead_count, cmt, cnt, upt_time]
        c_id = 0
        c_dict = dict() # record how many country has been recorded
        for d in data_stream: # # d is dict
            temp_cnt = d[cnt]
            if len(c_dict) == 0: 
                c_dict[temp_cnt] = c_id
            elif temp_cnt not in c_dict.keys(): 
                c_id += 1
                c_dict[temp_cnt] = c_id
            
            cursor.execute('select province_id from prov_lkup where provinceName = "{}"'.format(d[prv]))
            temp_prv_id = cursor.fetchall()[0][0]
            
            # this is for newly added item: current confirmed people
            if c_cfm_count in d.keys():
                temp_cur_confirmed = d[c_cfm_count]
            else: 
                temp_cur_confirmed = ''

            # this is for (might) newly added item: comment
            if cmt in d.keys():
                temp_comment = d[cmt]
            else: 
                temp_comment = ''
            
            # this is for None value in the suspectedCount
            temp_suspected_count = d[spct_count]
            if temp_suspected_count is None:
                temp_suspected_count = ''
            
            one_line_data = [temp_prv_id, d[prv], d[prv_sh], c_dict[temp_cnt], temp_cnt, temp_cur_confirmed, d[cfm_count], temp_suspected_count, d[cured_count], d[dead_count], temp_comment, _int_2_time(d[upt_time])]
            
            cursor.execute(_micro_insert_data(one_line_data, tbl_name))
            
def create_tbl_city_data(cursor, data_stream = ''): 
    '''
    input: is a list of dict, if dict does not have cities as key word, skip; default is empty
    output: to create and insert the province data in it
    '''
    tbl_name = 'city_data'

    item_type_dict = {'string': [city, cmt], 
        'float': [c_cfm_count, cfm_count, spct_count, cured_count, dead_count],
        'int': [city_id, prv_id],
        'date': [upt_time]
        }

    # upt_time: get from province data level; 
    # prv_id: get from the query
    # cmt/ c_cfm_count: maybe None
    list_title = [prv_id, city_id, city, c_cfm_count, cfm_count, spct_count, cured_count, dead_count, cmt, upt_time] 

    # create table
    sql_title_dict = _get_title_dict(item_type_dict)
    sql_sentence = _create_tbl(tbl_name, sql_title_dict, list_title)
    cursor.execute(sql_sentence)

    # insert data
    if data_stream:
        # [prv, prv_sh, c_cfm_count, cfm_count, spct_count, cured_count, dead_count, cmt, cnt, upt_time]
        c_id = 0
        c_dict = dict() # record how many country has been recorded
        for d in data_stream: # # d is dict
            temp_cnt = d[cnt]
            if len(c_dict) == 0: 
                c_dict[temp_cnt] = c_id
            elif temp_cnt not in c_dict.keys(): 
                c_id += 1
                c_dict[temp_cnt] = c_id
            
            cursor.execute('select province_id from prov_lkup where provinceName = "{}"'.format(d[prv]))
            temp_prv_id = cursor.fetchall()[0][0]
            
            # this is for newly added item: current confirmed people
            if c_cfm_count in d.keys():
                temp_cur_confirmed = d[c_cfm_count]
            else: 
                temp_cur_confirmed = ''

            # this is for (might) newly added item: comment
            if cmt in d.keys():
                temp_comment = d[cmt]
            else: 
                temp_comment = ''
            
            # this is for None value in the suspectedCount
            temp_suspected_count = d[spct_count]
            if temp_suspected_count is None:
                temp_suspected_count = ''
            
            one_line_data = [temp_prv_id, d[prv], d[prv_sh], c_dict[temp_cnt], temp_cnt, temp_cur_confirmed, d[cfm_count], temp_suspected_count, d[cured_count], d[dead_count], temp_comment, _int_2_time(d[upt_time])]
            
            cursor.execute(_micro_insert_data(one_line_data, tbl_name))

def _get_title_dict(item_type_dict): 
    '''
    item_type_dict: {'string': [fd1, fd2, ...], 'int': [fd3, fd4, ...]}
    output: sql_title_dict: {fd1: 'VARCHAR(50)', fd2: 'INTEGER', ...}
    '''
    sql_type_dict = {'string': 'VARCHAR(50)', 'int': 'INTEGER', 'date': 'DATETIME', 'float': 'DECIMAL(8, 3)'}
    sql_title_dict = dict()

    for k, v in item_type_dict.items(): 
        for i in v: 
            sql_title_dict[i] = sql_type_dict[k]
    return sql_title_dict

def _create_tbl(tbl_name, sql_title_dict, list_title, key_id = ''): 
    '''
    tbl_name: what the table name is
    sql_title_dic: {fd1: data_type, fd2: data_type, ...}
    list_title: ['fd1', 'fd2', ...]
    key_id: the table's key id, which could not be null, default is the list_title[0]

    output: sql sentence to use instantly.
    '''
    sql_command_text = ''
    start_command = 'CREATE TABLE IF NOT EXISTS {tbl_name} ('.format(tbl_name = tbl_name)
    end_command = ') ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;'

    sql_command_text = start_command

    if key_id == '': key_id = list_title[0]

    for i in list_title: 
        if i == key_id: 
            sql_command_text += i + ' ' + sql_title_dict[i] + ' NOT NULL, '
        else:
            sql_command_text += i + ' ' + sql_title_dict[i] + ', '
    sql_command_text = sql_command_text[:-2]
    sql_command_text += end_command
    return sql_command_text

def create_tbl_prov_lkup(cursor, data_stream = ''): 
    '''
    data_stream: is a list; default is empty
    return: dict of province name and id {prov1: prov_id1, prov2: prov_id2, ...}
    to create the province, and create the province id for other table use
    '''
    tbl_name = 'prov_lkup'

    item_type_dict = {'string': [cnt, prv, cmt], 
        
        'int': [cnt_id, prv_id]
        }
    list_title = [prv_id, prv, cnt_id, cnt, cmt]

    # create table
    sql_title_dict = _get_title_dict(item_type_dict)
    sql_sentence = _create_tbl(tbl_name, sql_title_dict, list_title)
    cursor.execute(sql_sentence)

    prov_lkup_dict = dict()
    # insert data
    
    if data_stream: 
        p_id = 0
        for d in data_stream: 
            one_line_data = [p_id, d, '', '', '']
            # print (one_line_data)
            prov_lkup_dict[d] = p_id 
            cursor.execute(_micro_insert_data(one_line_data, tbl_name))
            p_id += 1

    # return prov_lkup_dict

def _micro_insert_data(one_line_value, tbl_name): 
    '''
    one_line_value:['fd1_value', 'fd2_value', ...]
    '''
    # add quote on the value
    cnt_ls = ["'"+str(i)+"'" if len(str(i)) > 0 else 'NULL' for i in one_line_value] 
    cnt = ', '.join(cnt_ls)
    one_line_insert = 'INSERT INTO {tbl_name} VALUES ({content});'.format(tbl_name = tbl_name, content = cnt)
    # print (one_line_insert)
    return one_line_insert

def _int_2_time(data): 
    your_dt = datetime.fromtimestamp(int(data)/1000)
    return your_dt

def plot_by_diff_level(df, level_col, fig_name = 'test.png'): 
    '''
    df: dataframe
    level_col: col name 
    '''

    fontP = font_manager.FontProperties()
    fontP.set_family('SimHei')
    fontP.set_size(14)

    levels = df[level_col].unique() # different level in similar list format[0 1]
    colormap = cm.viridis
    colorlist = [colors.rgb2hex(colormap(i)) for i in np.linspace(0, .9, len(levels))]
    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    for i, v in enumerate(levels): 
        temp_df = df.loc[df[level_col] == v, ]
        temp_label = temp_df[prv].iloc[0]
        # print (temp_label)
        
        ax1.scatter(x = temp_df[dt_time], y = temp_df[cured_count], c = colorlist[i], label = temp_label, alpha = .5)
        # print (i)
        # print (temp_df[prv][0])

    plt.xlim(df[dt_time].min() - pd.Timedelta(days=1), df[dt_time].max()+pd.Timedelta(days=1))

    plt.legend(loc = 'upper left', prop = fontP)
    plt.savefig('test.png')
 


if __name__ == '__main__': 
    # # ==============================
    # # province name part
    # # ==============================
    # # update the province name 
    # # fl name is: province_name.json
    # update_province_name()

    # province_name = json.load(open('province_name.json', 'r', encoding = 'utf8'))['results']
    
    # # ==============================
    # # province data part
    # # ==============================
    # # get data
    # get_json_file(raw_data_fl)
    province_data = json.load(open('nCoV_data.json', 'r', encoding = 'utf8'))['results']
    
    #======================================
    # DATABASE PART
    #======================================
    sqluser = os.environ['sqluser']
    sqlpwd = os.environ['sqlpwd']
    sqlhost = os.environ['sqlhost']

    # # create ncov_data db if not exists
    # db1 = mysql.connect(host = sqlhost, user = sqluser, passwd = sqlpwd)
    # cursor = db1.cursor()
    # create_ncov_db(cursor)

    db1 = mysql.connect(host = sqlhost, user = sqluser, passwd = sqlpwd, db = 'ncov_data', use_unicode=True, charset="utf8")
    cursor = db1.cursor()
    
    # # create & insert data for province lookup table if not exists
    # create_tbl_prov_lkup(cursor, province_name)


    # # create province_table and insert data if not exists
    # create_tbl_prov_data(cursor, province_data)
    
    # create cities_table and insert data if not exists
    create_tbl_city_data(cursor)
    
    # # create table for city level

    # # create table for country level


    # # get the data for pandas plot
    # select_sql = 'select *, date(updateTime) as datetime from prov_data where province_id;'
    # df_all = pd.read_sql(select_sql, con = db1)
    

    db1.commit()
    db1.close()


    # # ======================================
    # # plot part
    # # ======================================
    
    # # make the col # shorter
    # col_list = [prv_id, prv, cfm_count, dead_count, cured_count, upt_time, dt_time]
    # df_short = df_all[col_list]
    
    # # remove the duplicate data
    # df_short = df_short.sort_values([prv_id, upt_time])
    # df_short = df_short.drop_duplicates([prv_id, dt_time], keep='last')

    # # plot the image
    # plot_by_diff_level(df_short, prv_id)
    
    # # sort by pro_id and time -> drop data and only remain the last data in that date

