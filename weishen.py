import time
import hashlib
import traceback
import json
import requests
import pymysql

#：HTTP 401 表示当前请求需要用户验证
#415状态码指的是当前请求头的类型 No support （不支持）
def get_rick_data():
    '''获取卫健委风险地区的数据'''
    # 获取时间戳和加密过后的字符串
    ts,headers_sign,xwif=gen_params()
    # 为Headers添加新的参数
    headers={
        'Cookie':"__asc=a06e0fea184add38d36fe8a9d48; __auc=a06e0fea184add38d36fe8a9d48; wdcid=62a3593ae686905e; wdses=1c1b57cfde6f4cd8; _gscu_1088464070=693636822coqsj91; _gscbrs_1088464070=1; acw_tc=2760824616693656635723409eaa33bcc76a5cba0531415707a33744b2e0b9; wdlast=1669367336; _gscs_1088464070=69363682q1s64191|pv:10; SERVERID=5bc7c6a063072f166ecee3b3037f193a|1669367337|1669365664",
        'Host': 'bmfw.www.gov.cn',
        'origin': 'http://bmfw.www.gov.cn/',
        'Referer': 'http://bmfw.www.gov.cn/yqfxdjcx/risk.html',
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.56",
        'X-Requested-With': 'XMLHttpRequest',
        'x-wif-nonce': 'QkjjtiLM2dCratiA',
        'x-wif-paasid': 'smt-application',
        'x-wif-signature': xwif,
        'x-wif-timestamp': ts


    }
    risk_url='http://bmfw.www.gov.cn/bjww/interface/interfaceJson'
    data={
        'appId':"NcApplication",
        'key':"3C502C97ABDA40D0A60FBEE50FAAD1DA",
        'nonceHeader':"123456789abcdefg",
        'paasHeader':"zdww",
        'signatureHeader':headers_sign,
        'timestampHeader':ts
    }
    # print(data)
    risk_resp=requests.post(risk_url,json=data,headers=headers)
    return risk_resp


def mysql():
    db = pymysql.connect(host='localhost', user='root', password='123456', database='covid', charset='utf8')
    cursor = db.cursor()
    return db,cursor


def gen_params():
    """生成加密参数"""
    # 生成signatrueHeader
    t=str(int(time.time())) # 需要秒级时间戳
    r="23y0ufFl5YxIyGrI8hWRUZmKkvtSjLQA" # 固定值
    s="123456789abcdefg" # 固定值
    s1=hashlib.sha256()
    s1.update((t+r+s+t).encode())
    headers_sign=s1.hexdigest().upper()
    y='fTN2pfuisxTavbTuYVSsNJHetwq5bJvCQkjjtiLM2dCratiA'
    s2 = hashlib.sha256()
    s2.update((t + y + t).encode())
    xwif = s2.hexdigest().upper()
    return t,headers_sign,xwif

def get_json():
    data = get_rick_data()
    highlist_ = data.json()['data']['highlist']
    lowlist_ = data.json()['data']['lowlist']
    time = data.json()['data']['end_update_time']
    risk_list = []
    for type,list, in zip(("高风险","低风险"),(highlist_,lowlist_)):
        for hd in list:
            province = hd['province']  # 省份
            city = hd['city']  # 市
            county = hd['county']  # 县
            for x in hd['communitys']:  # 详细地址
                risk_list.append([time, province, city, county, x, type])
    insert_data(risk_list)
"""cursor.execute("DROP TABLE IF EXISTS risk_area")
        create_sql="create table risk_area(end_update_time varchar (255),province varchar (255),city varchar (255)" \
                   ",county varchar (255),address varchar (1024),type varchar (255)) DEFAULT CHARSET=utf8"
        cursor.execute(create_sql)
        conn.commit()"""
def insert_data(data):
    conn,cursor = mysql()
    try:
        sql = """
        insert into risk_area
        (end_update_time, province, city, county, address, type)
        values(%s,%s,%s,%s,%s,%s)
        """
        sql_query = """
        select %s=(
        select end_update_time from risk_area
        order by end_update_time desc limit 1
        )
        """  # 查询输入的时间是否存在
        cursor.execute(sql_query, data[0][0])
        if not cursor.fetchone()[0]:  # 如果没有相应时间的记录，这里会返回None
            for item in data:
                cursor.execute(sql, item)  # 写入数据库
                # 提交事务
                conn.commit()
                print(f'写入 {item} 成功！')
    except:
        # 数据库回滚
        conn.rollback()
        traceback.print_exc()  # 在终端打印错误信息
    finally:
        conn.close()
        cursor.close()



if __name__ == '__main__':
    get_json()