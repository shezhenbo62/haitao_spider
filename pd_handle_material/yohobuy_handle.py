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
    yoho = db['yoho']
    weekend = datetime.date.today()
    yoho = pd.DataFrame(list(yoho.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    # 增加categort_id列
    # 3为服饰类别
    yoho['categort_id'] = 16
    # 删除_id列
    yoho = yoho.drop(columns=['_id'])
    # 清洗无关字符
    yoho = yoho.replace(r"\\n", "", regex=True)
    yoho = yoho.replace("¥", "", regex=True)
    # url绝对化
    yoho['pro_website'] = 'https:'+yoho.pro_website
    # 过滤掉折扣力度小的信息
    yoho = yoho.loc[yoho['discount'] != 1]
    # 按照品牌分组并进行分组排序取TOP50
    yoho = yoho.groupby(['brand_name']).apply(sort_data)
    # 根据link去重
    yoho = yoho.drop_duplicates('pro_website', 'first')
    # 部分字段json序列化
    yoho1 = yoho[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_yoho = pd.Series(yoho1)
    df_yoho = pd.DataFrame(sr_yoho, columns=['product_atlas'])
    yoho = yoho.reset_index(drop=True)
    result = pd.concat([yoho, df_yoho], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    yoho_pro = result.groupby(['brand_name']).apply(merge_js)
    yoho_pro = pd.DataFrame(yoho_pro, columns=['product_atlas'])
    yoho_pro = yoho_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    yohobuy_data = pd.merge(yoho_pro, result, how='inner', on=['brand_name'])
    yohobuy_data = yohobuy_data.drop(columns=['pro_pic'])

    # # 数据写入，可写入表格后利用mysql可视化工具导入到mysql数据库
    # yohobuy_data.to_excel('C:/Users/Administrator/Desktop/{}yohobuy_result.xls'.format(weekend), index=False)

    # 也可直接写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    db = pymysql.connect(host='192.168.2.199', user='root', password='root', db='d88_2.0_test', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    yohobuy_list = yohobuy_data.values.tolist()
    for info in yohobuy_list:
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
