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
    far = db['farfetch']
    weekend = datetime.date.today()
    far = pd.DataFrame(list(far.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    # 增加categort_id列
    # 3为服饰类别
    far['categort_id'] = 16
    # 删除_id列
    far = far.drop(columns=['_id'])
    far = far.loc[far['discount'].str.contains('OFF')]
    far['discount'] = far['discount'].replace("% OFF", "", regex=True)
    far['discount'] = far['discount'].astype('float64')
    far['discount'] = far['discount'] / 100
    # 按照品牌分组并进行分组排序取TOP50
    far = far.groupby(['brand_name']).apply(sort_data)
    # 根据pro_website去重
    far = far.drop_duplicates('pro_website', 'first')
    # 部分字段json序列化
    far1 = far[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_far = pd.Series(far1)
    df_far = pd.DataFrame(sr_far, columns=['product_atlas'])
    far = far.reset_index(drop=True)
    result = pd.concat([far, df_far], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    far_pro = result.groupby(['brand_name']).apply(merge_js)
    far_pro = pd.DataFrame(far_pro, columns=['product_atlas'])
    far_pro = far_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    far_data = pd.merge(far_pro, result, how='inner', on=['brand_name'])
    far_data = far_data.drop(columns=['pro_pic'])

    # # 数据写入，可写入表格后利用mysql可视化工具导入到mysql数据库
    # far_data.to_excel('C:/Users/Administrator/Desktop/{}farfetch_result.xls'.format(weekend), index=False)

    # 也可直接写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    db = pymysql.connect(host='192.168.2.199', user='root', password='root', db='d88_2.0_test', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    far_list = far_data.values.tolist()
    # print(far_list)
    for info in far_list:
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
