import numpy as np
# numpy是大部分python科学计算库的基础，多用于大型，多维数组上执行数值运算
import matplotlib.pyplot as plt

"""
    数组中存储的数据元素类型必须是统一类型
      - 优先级
        - 字符串> 浮点型 >整形
    不像列表，可以随意
    
"""
# arr = np.array([1, 2, 3])# 一维数组
# # print(arr)
# arr=np.array([[1,2,3],[4,5,6]])# 二维数组
# # print(arr)
# arr=np.array([1,2.2,"three"])
# print(arr) #['1' '2.2' 'three'] 都被转为字符串


# imread_arr = plt.imread("./static/img/1.jpg")# 返回的数组，数组转载的就是图片内容
# # print(imread_arr)
# plt.imshow(imread_arr)
# plt.show()

# print(np.ones(shape=(3, 4)))
# print(np.linspace(0, 100, num=20)) # 一维的等差数列数组
print(np.arange(10, 50, step=2)) # 一维等差数列