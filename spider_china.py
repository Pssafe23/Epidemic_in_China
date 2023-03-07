import requests
import json
import time
import pymysql
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 爬取腾讯疫情数据，自己写的一个只对广州的
def get_url():
    url = "https://api.inews.qq.com/newsqa/v1/query/pubished/daily/list?adCode=440100&limit=30"
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.52',
        'referer': 'https://news.qq.com/zt2020/page/feiyan.htm#/'
    }
    response = requests.get(url=url, headers=headers)
    data_ = json.loads(response.text)['data']
    postid = []
    for list in data_:  # 进行存储到列表
        postid.append(list)
    # # 1. 创建文件对象
    # with open('yiqing.csv','w',encoding='utf-8') as f:
    #     # 2. 基于文件对象构建 csv写入对象
    #     header = postid[0].keys()
    #     writer=csv.DictWriter(f,fieldnames=header)
    #     writer.writeheader()
    #     # 3. 构建列表头
    #     # writer.writerow(['y','date','city','confirm','dead','heal','suspect','adcode',
    #     #                  'confirm_add','yes_confirm_add','today_confirm_add','yes_wzz_add','today_wzz_add','is_show_wzz_add'])
    #     datas = []
    #     for li in postid:
    #         datas.append(li)
    #         writer.writerows(datas)

# 获取条数
def get_limit(month):
    today = datetime.now()
    pre_date = today - relativedelta(month=month)
    res_date = datetime(pre_date.year, pre_date.month, 1)
    limit=(today-res_date).days
    return limit,res_date


def get_url2():
    url = "https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=chinaDayListNew,chinaDayAddListNew"

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.52',
    }

    limit,res_date = get_limit(3)
    params={
        "limit":str(limit+1)
    }
    response = requests.get(url=url,params=params ,headers=headers)
    DayListdata = response.json()['data']

    history={}
    DayListNew = DayListdata['chinaDayListNew']
    for i in DayListNew:
        ds=(i['y']+'.'+i['date']).replace('.','-')
        history['ds']={
            'confirm':i['confirm'],
            "dead": i['dead'],
            "heal":i['heal'],
            "nowConfirm": i['nowConfirm'],
            "importedCase": i['importedCase']
        }
        print(history['ds'])
    DayAddListNew = DayListdata['chinaDayAddListNew']
    for i in DayAddListNew:
        ds=(i['y']+'.'+i['date']).replace('.','-')
        if ds in history.keys():
            history['ds'].update({
                'confirm_add': i['confirm'],
                "dead_add": i['dead'],
                "heal_add": i['heal'],
                "nowConfirm_add": i['nowConfirm'],
                "importedCase_add": i['importedCase']
            })
        print(history['ds'])

# 连接mysql
def con_mysql():
    db = pymysql.connect(host="localhost", user="root", password="123456", database="covid", charset='utf8')
    cur = db.cursor()
    return db, cur

# 获取json数据
def get_json(url):
    headers={
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.52'
    }
    response = requests.get(url=url, headers=headers)
    return json.loads(response.text)['data']

# 获取当日数据
def get_now(data):
    now=[]
    data_time = str(data["diseaseh5Shelf"]["lastUpdateTime"])# 数据更新的时间
    data_all = data["diseaseh5Shelf"]["areaTree"][0]
    data_province_s = data["diseaseh5Shelf"]["areaTree"][0]["children"]

    # 获取全国今日新增，累计确诊，治愈人数，死亡人数
    confirms=data_all['total']['confirm']
    nowConfirm=data_all['today']['confirm']
    heals=data_all['total']['heal']
    deads=data_all['total']['dead']

    # 获取每个省份的每个城市今日新增，累计确诊，治愈人数，死亡人数
    for data_province in data_province_s:
        province=data_province['name'] # 省份
        for data_city in data_province['children']:
            city=data_city['name'] # 城市
            confirm=data_city['total']['confirm'] # 确诊
            confirm_add=data_city['today']['confirm'] # 新增
            heal = data_city['total']['heal'] # 治愈
            dead = data_city['total']['dead'] # 死亡
            now.append((data_time,province,city,confirm_add,confirm,heal,dead))

    return  confirms,nowConfirm,heals,deads,now

# 获取历史数据
def get_past(data):
    past={}
    for data_day in data:
        data_time=data_day['date'] #获取最原始的时间
        time_deal = time.strptime(data_time, '%m.%d') # 根据指定的格式把一个时间字符串解析为时间元组
        date_1 = time.strftime('%m-%d', time_deal) # 重新组成新的时间字符串
        date=str(data_day['y'])+"-"+date_1

        past[date]={
            'confirm':data_day['confirm'], # 确诊
            'suspect':data_day['suspect'], # 疑似
            'heal':data_day['heal'],  # 治愈
            'dead':data_day['dead'] # 死亡
        }

    return past

# 写入当日数据
def insert_now(now):
    db ,cur = con_mysql()

    try:
        cur.execute("DROP TABLE IF EXISTS 当日数据")
        # 创建表语句
        insert_sql="create table 当日数据(时间 varchar (100),省份 varchar (50),城市 varchar (50),新增确诊 int (11)," \
                   "确诊人数 int (11),治愈人数 int (11),死亡人数 int (11))ENGINE=InnoDB DEFAULT CHARSET=utf8"
        # 执行sql语句
        cur.execute(insert_sql)
        #保存，必须有这句话
        db.commit()
        # 写入数据库
        save_sql="insert into 当日数据 values(%s,%s,%s,%s,%s,%s,%s)"
        cur.executemany(save_sql,now)
        db.commit()
        print('当日数据存储成功')
    except Exception as e:
        print('当日数据写入失败原因:%s' %e)

# 写入历史数据
def insert_past(past):
    db,cur = con_mysql()
    try:
        cur.execute("DROP TABLE IF EXISTS history")
        past_sql="create table history(ds varchar (100),confirm int (50),suspect int (50),heal int (50)," \
                 "dead int (50))ENGINE=InnoDB DEFAULT CHARSET=utf8"
        cur.execute(past_sql)
        db.commit()
        for date,data in past.items():
            sql_past=f"insert into history values('{date}',{data['confirm']},{data['suspect']},{data['heal']},{data['dead']})"
            cur.execute(sql_past)
        db.commit()
        print('历史数据写入成功')
    except Exception as e:
        print("数据写入失败原因：%s" %e)

# 写入历史新增数据
def insert_past_add(past):
    db,cur=con_mysql()
    try:
        cur.execute("DROP TABLE IF EXISTS 历史新增数据")
        past_sql="create table 历史新增数据(时间 varchar (100),确诊人数 int (50),疑似感染 int (50),治愈人数 int (50)," \
                 "死亡人数 int (50))ENGINE=InnoDB DEFAULT CHARSET=utf8"
        cur.execute(past_sql)
        db.commit()
        for date,data in past.items():
            #{date}必须加 ''
            sql_past=f"insert into 历史新增数据 values('{date}',{data['confirm']},{data['suspect']},{data['heal']},{data['dead']})"
            cur.execute(sql_past)
        db.commit()
        print("历史数据更新成功")
    except Exception as e:
        print("失败原因是:%s" %e)

# 写入累计确诊等四个关键数据
def insert_main(confirm, confirm_add, heal, dead):
    db,cur=con_mysql()
    try:
        cur.execute("DROP TABLE IF EXISTS 关键数据")
        sql_main="create table 关键数据(确诊人数 int(11),新增确诊 int(11),治愈人数 int(11),死亡人数 int(11))" \
                    "ENGINE=InnoDB DEFAULT CHARSET=utf8"
        cur.execute(sql_main)
        db.commit()
        save_sql=f"insert into 关键数据 values({confirm},{confirm_add},{heal},{dead})"
        cur.execute(save_sql)
        db.commit()
        print('关键数据写入成功')
    except Exception as e:
        print("数据写入失败原因：%s" %e)






def main():
    # 获取最新数据
    url1="https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=localCityNCOVDataList,diseaseh5Shelf"
    # 获取之前历史数据
    url2="https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=chinaDayListNew,chinaDayAddListNew&limit=30"
    data_1 = get_json(url1)
    data_2 = get_json(url2)
    data_3 = data_2['chinaDayAddListNew']
    data_4 = data_2['chinaDayListNew']
    confirms,nowConfirm,heals,deads,now= get_now(data_1)
    past = get_past(data_4)
    past_add = get_past(data_3)

    # 把数据插入数据库
    # insert_now(now)
    # 把历史数据插入数据库
    insert_past(past)
    # 把历史新增数据插入数据库
    # insert_past_add(past_add)
    # 把关键字插入数据库
    # insert_main(confirms,nowConfirm,heals,deads)


if __name__ == '__main__':
    dic={"s":{"q":[{"1":2},{"2":3}]}}
    # print(dic['s'])
    # print(dic["s"]["q"])
    # print(dic['s']["q"][0]["1"])
    # get_url2()
    main()
    # url2 = "https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=chinaDayListNew,chinaDayAddListNew&limit=30"
    # json1 = get_json(url2)
    # for date in json1['chinaDayListNew']:
    #     data_time = date['date']  # 获取最原始的时间
    #     time_deal = time.strptime(data_time, '%m.%d')  # 根据指定的格式把一个时间字符串解析为时间元组
    #     dat = time.strftime('%m-%d', time_deal)  # 重新组成新的时间字符串
    #     dd=date['y']+"-"+dat
    #     print(dd)




