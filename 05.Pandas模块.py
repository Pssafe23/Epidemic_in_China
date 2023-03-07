import pandas as pd  # 习惯性的导包方式
# Pandas最重要的两个概念：DataFrame、Series
# DataFrame：可以表示生活、项目中常见的二维表
df = pd.DataFrame({"Name": ["小明","大熊","张三",],
                "Age": [22, 35, 58],
                "Sex": ["male", "male", "female"]})
# print(df)
#
# print('-------------------------------------')
# # Series：DataFrame中的一列
# # Series也称为数据的序列，下面演示从DataFrame取出某个数据列
# # 这个数据列就是Series类型的
# sex = df['Sex']
# print(sex)  # 可以粗略理解为列表的另外一种形态
# print('统计男女数量：\n', sex.value_counts())
# # 转为列表
# print('将Series转为列表：', sex.tolist())  # ['male', 'male', 'female']
# # 作为序列元素，同样可以使用切片和索引
# print(sex[0])
# print(sex[1])
# print(sex[2])

# print('-------------------------------------')
# # 一般我们都不会从自己去写DataFrame，一般从数据库、本地文件中读取
# # pd.read_xxx 支持非常多格式的数据表的读取
# # 读取本地文件中的titanic.csv
# titanic = pd.read_csv('titanic.csv')
# print(titanic)
# 从数据库读取
from pymysql import connect
# from sqlalchemy import create_engine
conn = connect(
    user='root',
    password='123456',
    database='covid',
    host='localhost'
)
# 参数1：sql语句
# 参数2：con，数据库的连接对象
# engine = create_engine('mysql+pymysql://root:123456@localhost:3306/covid?charset=utf8', echo=True)
# conn = engine.connect()
covid19 = pd.read_sql('select * from risk_area', con=conn)
print(covid19)
#
# print('-------------------------------------')
# # 获取特定数据列、行
# age = titanic['Age']  # 筛选出Age这一列
# print(age)
# age2 = titanic.Age
# print(age2)
# # 数据信息筛选，上面的 [ ] 写的可以理解为一种筛选条件
# sub_ti = titanic[['Name', 'Age', 'Survived']]
# print(sub_ti)
# # 还可以利用运算进行筛选
# # 筛选年龄大于35岁的乘客
# print(age > 35)  # 这里会获取一个布尔值Series
# above_35 = titanic[age > 35]
# print(above_35[['Name', 'Age', 'Survived']])
# print('-------------------------------------')
# # 在DataFrame中进行索引和切片
# # 注意：在DataFrame不能直接用 [] 来索引和切片
# # Pandas提供了两个语法：iloc和loc，这两个语法的条件都要写在[]里面
# # 无论是iloc还是loc语法，切片时逗号前代表筛选行，逗号后代表筛选列
# # iloc语法，表示数字索引和切片，遵从Python的切片和索引规则
# print(titanic.iloc[:5])  # 表示获取前五行
# print(titanic.iloc[:5, :3])  # 表示获取前五行的前三列
# # loc语法，表示针对标签的索引（所谓的标签就是行首、列首的文本值）
# # 注意：loc语法与Python的切片规则不一致，它是包含冒号后的内容的
# print(titanic.loc[:5])
# print(titanic.loc[:5, 'PassengerId':'Pclass'])  # 列标签只有文本值，所以不要用数值
#
# # 下面这个df，列索引不是数字
# df = pd.DataFrame({"Name": ["小明","大熊","张三",],
#                 "Age": [22, 35, 58],
#                 "Sex": ["male", "male", "female"]},
#                   index=['a','b','c'])
# print(df)
# print(df.loc[:"b"])  # 所以，loc只根据标签来进行索引
# # 另外还可以使用花式索引，花式索引就是能够无视列、行的顺序任意指定内容进行输出
# # 并且输出的顺序按照列表来
# print(titanic[['Name', 'Age', 'Survived']])
# print(titanic.iloc[[2,4,6,7], [0, 1, 3, 2]])
# print(titanic.loc[:9, ['Name', 'Age', 'Fare', 'Sex', 'Survived']])
#
# print('-------------------------------------')
# # 数据表合并，pandas提供了concat方法用于合并两个或多个DataFrame、Series
# s1 = pd.Series([0,1], index=['a','b'])
# s2 = pd.Series([2,3,4], index=range(3))
# s3 = pd.Series([5,6,7,8,9], index=[0,1,'a','b',3])
# # concat涉及的参数：
# # 参数1：list类型，表示列表中的内容参与合并
# # 参数2：Axis，默认为0，也就是合并的方向；设置为1则表示横向连接
# print(pd.concat([s1, s2]))
# # 合并的时候，如果对应索引没有内容，则直接设置为NaN
# print(pd.concat([s1, s2], axis=1))
# print(pd.concat([s1, s3], axis=1))
# # DataFrame合并也是类似
# import numpy as np
# df1 = pd.DataFrame(
#     np.random.randn(3, 4),
#     columns=['a','b','c','d'])
# df2 = pd.DataFrame(
#     np.random.randn(2, 3),
#     columns=['b','d','a'])
# print(df1)
# print(df2)
# print("df1和df2纵向向连接：\n", pd.concat([df1, df2]))
# print("df1和df2横向连接：\n", pd.concat([df1, df2], axis=1))
# print("df1和s2横向合并：\n", pd.concat([df1, s2], axis=1))
# print("df1和s2纵向向合并：\n", pd.concat([df1, s2]))
#
# print('-------------------------------------')
# # 分组语法，使用groupby方法，然后用它提供的聚合方法进行统计分析
# # 查看titanic中每个性别的平均年龄
# print(titanic[titanic['Sex']=='male'].mean()['Age'])  # 30.72664459161148
# # 使用groupby可以对每个分组统一进行运算
# g1 = titanic[['Sex', 'Age']].groupby('Sex')  # 先提取关键列，再进行分组
# print(g1)  # Groupby对象
# # Groupby对象可以使用一些统计分析的方法
# print(g1.mean())  # 计算男女分组的平均年龄
# print(g1.max())  # 计算男女分组的最大年龄
# print(g1.min())  # 计算男女分组的最小年龄
# print(g1.sum())  # 计算男女分组的年龄总和
# print(g1.size())  # 计算男女分组的数量
# # 也可以先对titanic分组，再统计后提取Age
# g2 = titanic.groupby('Sex')
# print(g2.mean()['Age'])  # 计算所有可以计算的数值列
# # g2也是一个类似DataFrame的内容，但是print不能直接看到结果
# print(g2['Age'].mean())
#
# # 需求：男女各自的存活比例
# # 对于两个以上的分组指标，只需要用列表包含即可
# g3 = titanic.groupby(['Sex', 'Survived'])
# # 计算分组数量，仍然使用size
# print(g3.size())  # 这里因为有两个分组，所以返回的内容是多级索引的Series
# # 获取多级索引的内容
# print(g3.size()['female'][1])  # 注意1不是索引，指的是Survived的标签
# # 二级索引可以直接转为DataFrame，一级索引Sex作为行索引；二级索引Survived作为列索引
# print(g3.size().unstack())