import traceback

import pymysql
import time
import requests
import json
from datetime import datetime

from dateutil.relativedelta import relativedelta


def mysql():
    db = pymysql.connect(host='localhost', user='root', password='123456', database='covid', charset='utf8')
    cur = db.cursor()
    return db, cur


def get_json(url):
    headers = {
        'user - agent': 'Mozilla / 5.0(WindowsNT10.0;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 80.0.3987.116Safari / 537.36'
    }
    response = requests.get(url=url, headers=headers).text
    data = json.loads(response)
    return data['data']


# 获取当日数据
def get_now(data):
    now = []
    data_time = str(data['diseaseh5Shelf']['lastUpdateTime'])  # 数据更新时间
    data_all = data['diseaseh5Shelf']['areaTree'][0]
    data_province_s = data['diseaseh5Shelf']['areaTree'][0]['children']

    # 获取全国今日新增、累计确诊、治愈人数、死亡人数
    confirms = data_all['total']['confirm']
    confirms_add = data_all['today']['confirm']
    heals = data_all['total']['heal']
    deads = data_all['total']['dead']

    # 获取每个省份的每个城市今日新增、累计确诊、治愈人数、死亡人数
    for data_province in data_province_s:
        province = data_province['name']  # 省份
        for data_city in data_province['children']:
            city = data_city['name']  # 城市
            confirm = data_city['total']['confirm']  # 确诊
            confirm_add = data_city['today']['confirm']  # 新增
            heal = data_city['total']['heal']  # 治愈
            dead = data_city['total']['dead']  # 死亡
            now.append((data_time, province, city, confirm_add, confirm, heal, dead))

    return confirms, confirms_add, heals, deads, now


# 获取历史数据
def get_past(data):
    past = {}
    for data_day in data:
        data_time = data_day['date']  # 获取最原始的时间
        time_deal = time.strptime(data_time, '%m.%d')  # 根据指定的格式把一个时间字符串解析为时间元组
        date = time.strftime('%m-%d', time_deal)  # 重新组成新的时间字符串
        past[date] = {
            'confirm': data_day['confirm'],  # 确诊
            'suspect': data_day['suspect'],  # 疑似
            'heal': data_day['heal'],  # 治愈
            'dead': data_day['dead']  # 死亡
        }

    return past


# 写入当日数据
def insert_now(now):
    db, cur = mysql()
    try:
        cur.execute("DROP TABLE IF EXISTS 当日数据")
        # 写创建表的sql语句
        set_sql_now = "create table 当日数据(时间 varchar(100),省份 varchar(50),城市 varchar(50),新增确诊 int(11)," \
                      "确诊人数 int(11),治愈人数 int(11),死亡人数 int(11))ENGINE=InnoDB DEFAULT CHARSET=utf8"
        # 执行sql语句
        cur.execute(set_sql_now)
        # 保存
        db.commit()
        # 写入数据库
        save_sql_now = "insert into 当日数据 values(%s,%s,%s,%s,%s,%s,%s)"
        cur.executemany(save_sql_now, now)  # now位置必须是个列表，列表里面的元素是数组
        db.commit()
        print('当日数据写入成功')
    except Exception as e:
        print('当日数据写入失败原因:%s' % e)


# 写入历史数据
def insert_past(past):
    db, cur = mysql()
    try:
        cur.execute("DROP TABLE IF EXISTS 历史数据")
        # 写创建表的sql语句
        set_sql_past = "create table 历史数据(时间 varchar(100),确诊人数 int(11),疑似病例 int(11),治愈人数 int(11)," \
                       "死亡人数 int(11))ENGINE=InnoDB DEFAULT CHARSET=utf8"
        # 执行sql语句
        cur.execute(set_sql_past)
        # 保存
        db.commit()
        # 写入历史数据
        for date, data in past.items():
            sql_past = f"insert into 历史数据 values('{date}',{data['confirm']},{data['suspect']},{data['heal']}," \
                       f"{data['dead']})"
            cur.execute(sql_past)
        db.commit()
        print('历史数据写入成功')
    except Exception as e:
        print('历史数据写入失败原因:%s' % e)


# 写入历史新增数据
def insert_past_add(past):
    db, cur = mysql()
    try:
        cur.execute("DROP TABLE IF EXISTS 历史新增数据")
        # 写创建表的sql语句
        set_sql_past = "create table 历史新增数据(时间 varchar(100),新增确诊 int(11),新增疑似 int(11),新增治愈 int(11)," \
                       "新增死亡 int(11))ENGINE=InnoDB DEFAULT CHARSET=utf8"
        # 执行sql语句
        cur.execute(set_sql_past)
        # 保存
        db.commit()
        # 写入历史数据
        for date, data in past.items():
            sql_past = f"insert into 历史新增数据 values('{date}',{data['confirm']},{data['suspect']},{data['heal']}," \
                       f"{data['dead']})"
            cur.execute(sql_past)
        db.commit()
        print('历史新增数据写入成功')
    except Exception as e:
        print('历史新增数据写入失败原因:%s' % e)

def cal_limit_days(month=3):
    '''计算3个月的天数，并且除了本月以外都是完整月份，'''
    today = datetime.now()
    # 计算month个月前的日期
    pre_date = today - relativedelta(months=3)
    print(pre_date) # 得到的是datetime类型的数据
    # 获取pre_date的完整月份，将天数改为1即可
    res_date = datetime(pre_date.year, pre_date.month, 1)
    # 计算res_date和今天相差的天数，即limit值
    limit = (today - res_date).days
    return limit, res_date
def turn_to_sql_date(i,min_date,year='y'):
    '''判断日期不要超过左边界（最小日期），然后转为sql语句能接受的格式'''
    ds=str(i[year])+'.'+i['date']
    ds_tmp=datetime.strptime(ds,'%Y.%m.%d')
    if ds_tmp<min_date:
        print(f'日期[{ds_tmp.date()}]超过了左边界，跳过')
        return None
    return ds_tmp.strftime('%Y-%m-%d')
def get_tencent_data():
    headers = {
        'user - agent': 'Mozilla / 5.0(WindowsNT10.0;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 80.0.3987.116Safari / 537.36'
    }
    '''获取全国数据'''
    # 计算并获取limit值
    limit, min_date = cal_limit_days()
    print('当前的limit值为：', limit)
    print('当前计算得到的最小日期为：', min_date.date())
    dayListUrl = 'https://api.inews.qq.com/newsqa/v1' \
                 '/query/inner/publish/modules/list?' \
                 'modules=chinaDayListNew,chinaDayAddListNew&limit=' + str(limit+ 1)
    chinaRes = requests.get(dayListUrl, headers=headers)
    dayListData = chinaRes.json()['data']
    # 按照日期整理到字典中，字典的键就是日期
    history = {}
    for i in dayListData['chinaDayListNew']: # 先获取统计的总体数据
        # 提取日期，作为history的键
       # ds = i['y'] + '.' + i['date']
        ds=turn_to_sql_date(i,min_date)
        if not ds:
            continue
        history[ds] = {
            'confirm': i['confirm'], 'confirm_now': i['nowConfirm'],
            'heal': i['heal'], 'dead': i['dead'], 'importedCase':i['importedCase']
        }
        #调试用
        #break
    for i in dayListData['chinaDayAddListNew']: # 先获取统计的总体数据
        # 提取日期，作为history的键
        #ds = i['y'] + '.' + i['date']
        ds=turn_to_sql_date(i,min_date)
        if ds not in history.keys():
            continue # 仅当日期存在于字典中时，才进行数据写入
        # 字典的update方法，可以实现更新键值对、插入键值对
        history[ds].update({
            'confirm_add': i['confirm'], 'heal_add': i['heal'],
            'dead_add': i['dead'], 'importedCase_add': i['importedCase']
        })
        # print(history)
    insert_into_history(history)
        #pprint(history)
        #调试用
        #break
def get_province_data():
    headers = {
        'user - agent': 'Mozilla / 5.0(WindowsNT10.0;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 80.0.3987.116Safari / 537.36'
    }
    '''获取各个省份的疫情数据'''
    # 获取areaTree中各个省份的信息
    dh5_url = 'https://api.inews.qq.com/newsqa/' \
              'v1/query/inner/publish/modules/'  \
              'list?modules=localCityNCOVDataList,diseaseh5Shelf'
    dh5_resp = requests.post(dh5_url, headers=headers)
    areaTreeData = dh5_resp.json()['data']['diseaseh5Shelf'] ['areaTree'][0]['children']
    gat_adcode = {'台湾':'710000', '香港':'810000', '澳门':'820000'} # 补充港澳台的adcode
    for pro_info in areaTreeData: # 拿到各个省份的数据
        # 当天剩余确诊人数，只有DH5这个表里面有，省份的api没有，所以现有确诊只写入当天的内容
        # 另外，当天最新的数据以DH5这表为准
        print("当前爬取的省份是：{}".format(pro_info['name']))
        ds_str = pro_info['date'].replace('/', '-')
        # 将每个省份的today和total分别提取出来
        today = pro_info['today']
        total = pro_info['total']
        province = pro_info['name'] # 省份名
        confirm = total['confirm'] # 累计确诊
        confirm_add = today['confirm'] # 新增确诊
        now_confirm = total['nowConfirm'] # 现存确诊人数
        heal = total['heal'] # 累计治愈
        heal_add = None # dh5today中没有新增治愈的数据
        dead = total['dead']
        dead_add = today['dead_add']
        # 获取各个省份的adcode，另外补充港澳台的adcode
        adCode = pro_info['adcode']
        if province in gat_adcode:
            adCode = gat_adcode[province]
        # print(f"当前的省份 [{province}] 代号是：{adCode}")
        # 获取各个省份的历史疫情数据
        # 计算并获取limit值
        limit, min_date = cal_limit_days()
        province_url = f'https://api.inews.qq.com/newsqa/v1/'  \
                       f'query/pubished/daily/list?adCode={adCode}&limit={limit}'
        province_resp = requests.get(province_url, headers=headers)
        pro_data_detail = province_resp.json()['data']
        # 补充dh5里面没有的heal_add的参数(仅限当天的数据)
        heal_add = pro_data_detail[-1]['newHeal']
        # 写入当天的数据
        insert_into_detail([
            ds_str,province,confirm,confirm_add,
            now_confirm,heal_add,heal,dead,dead_add
        ])
        for pro_day in pro_data_detail: # 每个省份当天的数据

            ds = turn_to_sql_date(pro_day, min_date, year='year') # 这里年份变为了year
            if not ds:
                continue
            # 获取各项数据
            if ds != ds_str:
                confirm = pro_day['confirm'] # 累计确诊
                confirm_add = pro_day['confirm_add'] # 新增确诊
                now_confirm = None # 现存确诊人数，这个api没有
                heal = pro_day['heal'] # 累计治愈
                heal_add = pro_day['newHeal'] # 新增治愈
                dead = pro_day['dead']
                dead_add = pro_day['newDead'] # 新增死亡
                insert_into_detail([
                    ds,province,confirm,confirm_add,
                    now_confirm,heal_add,heal,dead,dead_add
                ])
        #break

def insert_into_history(data):
    '''将输入插入到history表中'''
    conn,cursor=mysql()# 获取数据库链接的引警对象和游标对象
    try:
        sql="insert into history values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        sql_query="select confirm from history where ds=%s"# 查询某个口期的记录是否存在
        # history: {'2022-11-23': {'confirm':800w.....}}
        for k,v in data.items():
        # k就是口期，V是各项数据的字典
            if not cursor.execute(sql_query,k):# 先检测日期是否存在，不存在则写入
                cursor.execute(sql,[k,v.get('confirm'),
                                    v.get('confirm_add'),
                                    v.get('confirm_now'),
                                    v.get('heal'),v.get('heal_add'),
                                    v.get('dead'),v.get('dead_add'),
                                    v.get('importedCase'),
                                    v.get('importedCase_add')])
                print(f"[history] | [china] | {k} 记录写入成功!")
            # TODO 打印插入的内容
        # 提交事务
        conn.commit()
    except:
        # 数据库回滚
        conn.rollback()
    finally:
        conn.close()
        cursor.close()

def insert_into_detail(data):
    '''将输入插入到detail表中'''
    conn, cursor = mysql()  # 获取数据库链接的引警对象和游标对象
    try:
        sql = """
               insert into details
               (update_time,province,confirm,confirm_add,confirm_now,heal_add,heal,dead,dead_add)
               values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
               """
        sql_query = "select confirm from details where province=%s and update_time = %s "  # 查询重复需要省份和日期一起查
        if not cursor.execute(sql_query, [data[1], data[0]]):
            print(f'写入 [{data[1]}] | {data[0]} 成功！')
            cursor.execute(sql, data)  # 写入数据库
            # 提交事务
            conn.commit()

    except:
        # 数据库回滚
        conn.rollback()
        traceback.print_exc()  # 在终端打印错误信息

    finally:
        conn.close()
        cursor.close()

# 写入累计确诊等四个关键数据
def insert_main(confirm, confirm_add, heal, dead):
    db, cur = mysql()
    try:
        cur.execute("DROP TABLE IF EXISTS 关键数据")
        # 写创建表的sql语句
        set_sql_main = "create table 关键数据(确诊人数 int(11),新增确诊 int(11),治愈人数 int(11),死亡人数 int(11))" \
                       "ENGINE=InnoDB DEFAULT CHARSET=utf8"
        cur.execute(set_sql_main)
        db.commit()
        # 写入确诊人数、新增确诊、治愈人数、死亡人数的数据
        save_sql_main = f"insert into 关键数据 values({confirm},{confirm_add},{heal},{dead})"
        cur.execute(save_sql_main)
        db.commit()
        print('关键数据写入成功')
    except Exception as e:
        print('关键数据写入失败原因:%s' % e)


def main():
    # get_tencent_data()
    get_province_data()
    # # 用于请求获得每日最新数据
    # url_1 = 'https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=statisGradeCityDetail,diseaseh5Shelf'
    # # 用于获取以前的历史数据
    # url_2 = 'https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=chinaDayList,chinaDayAddList,nowConfirmStatis,provinceCompare'
    # data_1 = get_json(url_1)
    # data_2 = get_json(url_2)
    # data_3 = data_2['chinaDayList']
    # data_4 = data_2['chinaDayAddList']
    # # 数据筛选
    # confirms, confirms_add, heals, deads, now = get_now(data_1)   # 当日数据筛选
    # past = get_past(data_3)  # 历史累计数据筛选
    # past_add = get_past(data_4)  # 历史每日数据筛选
    # # 把数据插入到数据库
    # insert_now(now)
    # insert_past(past)
    # insert_past_add(past_add)
    # insert_main(confirms, confirms_add, heals, deads)


if __name__ == '__main__':
    main()
