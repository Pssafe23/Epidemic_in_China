import pandas as pd  # 习惯性的导包方式
from datetime import datetime
from flask import Flask,render_template,jsonify
import sqldata2
from dateutil.relativedelta import relativedelta
from pymysql import connect
app = Flask(__name__)


def get_conn():
    conn=connect(
        user='root',
        password='123456',
        database='covid',
        host='localhost'
    )
    cursor=conn.cursor()
    return conn,cursor

def close_con(con,cursor):
    con.close()
    cursor.close()

def query(sql):
    con,cursor=get_conn()
    cursor.execute(sql)
    res=cursor.fetchall()
    close_con(con,cursor)
    return res

@app.route('/')
def index():
    sql="""
    select 确诊人数,治愈人数,新增确诊,死亡人数 from 关键数据
    """
    res=query(sql)[0]
    data={
        'confirm_add':res[0],
        'heal_add':res[1],
        'confirm_now':res[2],
        'confirm':res[3],
    }
    return render_template('index.html',data=data)

@app.route('/get_risk_info')
def get_risk_info():
    '''风险地区信息提取'''
    # 获取高低风险地区的数量
    h_res = query("select count(*) from risk_area where type='高风险'")[0][0]
    l_res = query("select count(*) from risk_area where type='低风险'")[0][0]
    risk_num = {'high_num': h_res, 'low_num': l_res}
    # 风险地区信息和风险类型的列表
    sql = """
    select * from risk_area
    """
    res = query(sql)
    risk = []  # 风险类型
    detail = []  # 详细地址
    for end_update_time, province, city, county, address, risk_type in res:
        # print(risk_type)
        risk.append(risk_type)
        detail.append(f"{province}\t{city}\t{county}\t{address}")
    return jsonify({
        "details": detail,
        "risk": risk,
        "risk_num": risk_num,
        "update_time": res[0][0]  # 因为每条记录的时间都是一样的
    })

@app.route('/get_top5')
def top5():
    sql="""
    select 省份,确诊人数 from 当日数据
    where 时间=(
    select 时间 from 当日数据
     order by 时间 desc limit 1)
     order by 确诊人数 desc limit 5
    """

    cityList=[]
    cityData=[]
    res=query(sql)
    for province,count in res:
        cityList.append(province)
        cityData.append(count)
    # print(res)
    return jsonify({
        'cityList':cityList,
        'cityData':cityData,
    })

@app.route('/get_heal_dead')
def heal_dead():
    sql="select ds,dead,dead_add,heal,heal_add from history order by ds"
    con=get_conn()[0]
    df=pd.read_sql(sql,con=con)
    #按照每个月分组
    # df['ds'] = pd.to_datetime(df['ds'], errors='coerce')
    df['year_month']=df.ds.dt.to_period('M')
    # print(df['year_month'])
    g=df.groupby("year_month")
    # print(g.groups.keys())
    # 新增的数据
    healadd=g['heal_add'].sum()
    # print(healadd)
    deadadd=g['dead_add'].sum()
    # print(deadadd)
    dateList=list(map(str,g.groups.keys()))
    indices=[x[-1] for x in g.groups.values()]
    # print(indices)
    # print(df)
    # print(g.groups)
    df_tmp=df.loc[indices,['ds','heal','dead']]
    return jsonify({
        'addData':{
            'deadAdd':deadadd.tolist(),
            'healAdd':healadd.tolist()
          },
          'dateList':dateList,
          'sumData':{
            'dead':df_tmp['dead'].tolist(),
            'heal':df_tmp['heal'].tolist()
          }
    })

@app.route('/get_two_month')
def get_two_month():
    pre_date=datetime.now()-relativedelta(month=1)
    res_date=datetime(pre_date.year,pre_date.month,1)
    # 获取dataframe
    con=get_conn()[0]
    df=pd.read_sql('select ds,confirm_add,importedCase_add from history',con=con)
    df_tmp=df[df.ds>=res_date]
    dateList=df_tmp.ds.astype('str').tolist()
    confirmAddList=df_tmp.confirm_add.tolist()
    importCaseList=df_tmp.importedCase_add.tolist()
    return jsonify({
        "dateList":dateList,
        'confirmAddList':confirmAddList,
        'importedCaseList':importCaseList
    })

@app.route('/get_death_rate')
def get_death_rate():
    sql='select 确诊人数,死亡人数 from 关键数据'
    data = query(sql)
    confirm=data[0][0]
    dead=data[0][1]
    dead_rate=(dead/confirm)*100
    dead_rate = round(dead_rate, 2)
    # print(dead_rate)

    return jsonify({
        'dead':dead,
        'confirm':confirm,
        'dead_rate':dead_rate
    })


@app.route('/get_now_confirm')
def get_now_confirm():
    con=get_conn()[0]
    sql="""
select province, confirm from details
where update_time=(
select update_time
from details
order by update_time desc limit 1)
"""
    df = pd.read_sql(sql, con)
    # print(df.size)
    # print(df[df.confirm<1000].count().tolist()[0])
    # print(df['province'].tolist())
    # provinces = df['province'].tolist()
    # print(df['confirm'].sum())
    df['confirm'].sum()
    # print(df.values.tolist())
    # print(len(df.values.tolist()))
    length = len(df.values.tolist())
    # print(df.values.tolist()[length-1])
    # print(df)
    # g = df.groupby('province')
    # print(len(g.groups.keys()))
    # indices = [x[-1] for x in g.groups.values()]
    # # print(len(indices))
    # cut = pd.cut(x=indices, bins=4, right=False)
    # # print(len(cut))
    # # print(cut)
    # # names=[x for x in g.groups.keys()]
    # # print(cut.)
    # # print(indices)
    d=[]
    for i in range(0,length):
        # sum_ = df.values.tolist()[i][1] / df['confirm'].sum()
        # f = round(sum_ * 100, 2)
        t={'name':df.values.tolist()[i][0],'value':df.values.tolist()[i][1]}
        d.append(t)
        # print(d[i])

    return jsonify(
        d
        # {'name':"[200,300000)",'value':12},
        # {'name':"[0,100)",'value':6},
        # {'name':"[100,200)",'value':2},
        # {'name':"[300000,400000)",'value':1}
        )



@app.route('/get_map_data')
def get_map():
    """各省每个月新增确诊地图"""
    con=get_conn()[0]
    sql='select update_time,province,confirm_add from details'
    df=pd.read_sql(sql,con)
    df['update_time']=df.update_time.dt.to_period('M')
    # 透视表，作用就是可以指定Dataframe表结构
    df_tmp=pd.pivot_table(df,index='province',
                          columns='update_time',values='confirm_add',
                          aggfunc='sum')#aggfunc 指定统计用的聚合函数
    year_month=df_tmp.columns.astype('str').tolist()
    province=df_tmp.index.tolist()
    # 这里按照月份给定数据即可
    confirm_add=[df_tmp[col].tolist() for col in year_month]
    return jsonify({
        'year_month':year_month,
        'province':province,
        'confirm_add':confirm_add
    })



# @app.route('/l1')
# def get_data_l1():
#     data=sqldata2.get_l1_data()
#     # 将日期，确诊，治愈，死亡人数写入列表，并返回json格式
#     date,confirm,heal,dead=[],[],[],[]
#     for i,j,m,n in data:
#         date.append(i)
#         confirm.append(j)
#         heal.append(m)
#         dead.append(n)
#     return jsonify({
#         'date':date,
#         'confirm':confirm,
#         'heal':heal,
#         'dead':dead
#     })
#
# @app.route('/l2')
# def get_data_l2():
#     data=sqldata2.get_l2_data()
#     date,confirm_add,heal_add=[],[],[]
#
#     for i,j,m in data:
#         date.append(i)
#         confirm_add.append(j)
#         heal_add.append(m)
#     return jsonify({
#         'date':date,
#         'confirm_add':confirm_add,
#         'heal_add':heal_add
#     })
#
# @app.route('/c1')
# def get_data_c1():
#     confirm,suspect,heal,dead=sqldata2.get_c1_data()
#     return jsonify({
#         'confirm':confirm,
#         'suspect':suspect,
#         'heal':heal,
#         'dead':dead
#     })
#
# @app.route('/c2')
# def get_data_c2():
#     data=sqldata2.get_c2_data()
#     result=[]
#     for items in data:
#         result.append({'name':items[0],'value':int(items[1])})
#     return jsonify({
#         'key_data':result
#     })
#
# @app.route("/r1")
# def get_data_r1():
#     data=sqldata2.get_r1_data()
#     province,confirm_add=[],[]
#     for i,j in data:
#         province.append(i)
#         confirm_add.append(int(j))
#     return jsonify({
#         'province':province,
#         'confirm_add':confirm_add
#     })



if __name__ == '__main__':
    app.run()
    #host="127.0.0.1",port=5000,threaded=True
