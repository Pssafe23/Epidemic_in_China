import pymysql
import time
import json
import requests

# 数据库连接
def mysql():
    db=pymysql.connect(host='localhost',user='root',password='123456',database='covid',charset='utf8')
    cur=db.cursor()
    return db,cur

# 获得json数据
def get_json(url):
    headers={
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.52'
    }
    response = requests.get(url=url, headers=headers)
    return response.json()['data']


# 获取全球目前数据
def global_data(data):
    womAboard = data['WomAboard']
    time=data['WomWorld']['lastUpdateTime']
    print(time)
    now=[]

    # 全球总共累计的确诊，治愈，死亡，新增
    now_confirm_ = data['WomWorld']['nowConfirm']
    nowConfirmAdd= data['WomWorld']['nowConfirmAdd']
    world_heal_ = data['WomWorld']['heal']
    world_dead_ = data['WomWorld']['dead']


    # 时间，哪个州，国家名，确诊，死亡，治愈，新增确诊，疑似
    for wom in womAboard:
        continent_ = wom['continent']
        countries=wom['name']
        confirm_ = wom["nowConfirm"]
        confirm_add = wom['confirmAdd']
        heal_ = wom['heal']
        dead_ = wom['dead']
        suspect_ = wom['suspect']
        now.append((time,continent_,countries,confirm_,confirm_add,heal_,dead_,suspect_))

    return now_confirm_,nowConfirmAdd,world_heal_,world_dead_, now

# 将目前数据存储到mysql
def insert_global(now):
    db,cur=mysql()
    try:
        cur.execute("DROP TABLE IF EXISTS gdata")

        insert_sql="create table gdata (time varchar (50),continent varchar (50),countries varchar (50)," \
                   "confirm int (15),confirm_add int (15),heal int (15),dead int (15),suspect int (15))ENGINE=InnoDB " \
                   "DEFAULT CHARSET=utf8"
        cur.execute(insert_sql)
        db.commit()
        save_sql="insert into  gdata values(%s,%s,%s,%s,%s,%s,%s,%s)"
        cur.executemany(save_sql,now)
        db.commit()
        print("全球数据插入成功")
    except Exception as e:
        print("失败原因是:%s" %e)


# 获取全球历史具体国家与城市数据
def global_past(data):
    FAutoforeignList = data['FAutoforeignList']
    # 获取国家，城市，确诊，死亡，治愈，新增确诊，疑似
    post=[]
    for fa in FAutoforeignList:
        date_ = fa['date']
        strptime = time.strptime(date_, '%m.%d')
        s = time.strftime("%m-%d", strptime)
        date=fa['y']+'-'+s
        if 'children' in fa:
            children_ = fa['children']
            for c in children_:
                countries=fa['name'],
                city=c['name'],
                confirm =c['confirm'],
                heal= c['heal'],
                dead=c['dead'],
                suspect= c['suspect']
                post.append((date,countries,city,confirm,heal,dead,suspect))
        else:
            countries = fa['name'],
            city = fa['name'],
            confirm = fa['confirm'],
            heal = fa['heal'],
            dead = fa['dead'],
            suspect = fa['suspect']
            post.append(date, countries, city, confirm, heal, dead, suspect)

    return post


if __name__ == '__main__':
    url3 = "https://api.inews.qq.com/newsqa/v1/automation/modules/list?modules=FAutoCountryConfirmAdd,WomWorld,WomAboard"
    url4="https://api.inews.qq.com/newsqa/v1/automation/modules/list?modules=FAutoforeignList"
    data = get_json(url4)
    # now = global_data(data)
    # for i in now:
    #     print(i)
    past = global_past(data)
    for v in past:
        print(v)
