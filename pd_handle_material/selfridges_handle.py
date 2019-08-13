# coding:utf-8
import pandas as pd
import pymongo
import datetime
import pymysql

"""
数据清洗及写入mysql数据库
"""


def sort_data(group):
    return group.sort_values(by=['offers', 'discount'], ascending=False)[:10]


def merge_js(group):
    return str(group.product_atlas.tolist())  # 列表数据类型转换为字符串，否则写入数据库会报错


def run():
    # 抓下来的数据统一存入mongodb再做清洗工作
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    selfridges = db['self']
    weekend = datetime.date.today()
    selfridges = pd.DataFrame(list(selfridges.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    # 增加categort_id列
    # 3为服饰类别
    selfridges['categort_id'] = 16
    # 删除_id列
    selfridges = selfridges.drop(columns=['_id'])
    # 按照品牌分组并进行分组排序取TOP50
    selfridges = selfridges.groupby(['brand_name']).apply(sort_data)
    # 根据pro_website去重
    selfridges = selfridges.drop_duplicates('pro_website', 'first')
    # 部分字段json序列化
    selfridges1 = selfridges[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_selfridges = pd.Series(selfridges1)
    df_selfridges = pd.DataFrame(sr_selfridges, columns=['product_atlas'])
    selfridges = selfridges.reset_index(drop=True)
    result = pd.concat([selfridges, df_selfridges], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    selfridges_pro = result.groupby(['brand_name']).apply(merge_js)
    selfridges_pro = pd.DataFrame(selfridges_pro, columns=['product_atlas'])
    selfridges_pro = selfridges_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    selfridges_data = pd.merge(selfridges_pro, result, how='inner', on=['brand_name'])
    selfridges_data = selfridges_data.drop(columns=['pro_pic'])

    # # 数据写入，可写入表格后利用mysql可视化工具导入到mysql数据库
    # selfridges_data.to_excel('C:/Users/Administrator/Desktop/{}selfridges_result.xls'.format(weekend), index=False)

    # 也可直接写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    db = pymysql.connect(host='192.168.2.199', user='root', password='root', db='d88_2.0_test', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    selfridges_list = selfridges_data.values.tolist()
    # print(selfridges_list)
    for info in selfridges_list:
        # 使用 execute()  方法执行 SQL
        sql = '''
        insert into d88_material (brand_name,product_atlas,create_time,discount,discounts_money,current_price,
        original_price,describtion,url,category_id)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        cursor.execute(sql, info)
        db.commit()
    db.close()


if __name__ == '__main__':
    run()
