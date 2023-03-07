import pymysql

def con_mysql():
    db = pymysql.connect(host="localhost", database="covid", user="root", password="123456", charset="utf8")
    cur=db.cursor()
    return db,cur

def close(db,cur):
    cur.close()
    db.close()

# 查询
def query(sql: str,*args):
    db,cur=con_mysql()
    cur.execute(sql,args)
    result=cur.fetchall()
    close(db,cur)
    return result

# 一个时间区间内，累计确诊，治愈，死亡人数
def get_l1_data():
    sql="""
    select 时间,确诊人数,治愈人数,死亡人数 from 历史数据
    """
    return query(sql)

# 一个时间区间内，新增确诊和治愈情况
def get_l2_data():
    sql="""
    select 时间,确诊人数,治愈人数 from 历史新增数据
    """
    return query(sql)

# 全国累计的确诊人数，新增确诊，治愈人数，死亡人数
def get_c1_data():
    sql="""
    select 确诊人数,新增确诊,治愈人数,死亡人数 from 关键数据
    """
    return query(sql)

# 全国以省份为单位的 新增确诊信息
def get_c2_data():
    sql="""
    select 省份,sum(新增确诊) from 当日数据
    group by 省份
    """
    return query(sql)

# 省/直辖市当日的新增确诊 前五
def get_r1_data():
# 此语句筛选出每个省份或直辖市，当日的新增总数
    sql="""
    select 省份,sum(新增确诊) as 新增确诊 from 当日数据
    group by 省份
    order by 新增确诊 desc limit 5
    """
    return query(sql)

if __name__ == '__main__':
    # data = get_l1_data()
    data = get_c1_data()
    for d in data:
        print(d)





