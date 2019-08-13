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


def func(cate):
    if cate == '母婴世界' or cate == '母婴产品':
        return 14
    elif cate == '营养保健' or cate == '环球美食' or cate == '食品饮料':
        return 8
    elif cate == '个护美妆' or cate == '美妆个护':
        return 4
    elif cate == '居家日用' or cate == '洗护日用':
        return 6
    elif cate == '箱包配饰':
        return 16


def run():
    # 抓下来的数据统一存入mongodb再做清洗工作
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    # bade = db['bade']
    bade = db['pharmacy']
    weekend = datetime.date.today()
    bade = pd.DataFrame(list(bade.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    # 过滤掉折扣力度小的信息
    bade = bade.loc[bade['discount'] <= 0.85]
    # 增加categort_id列
    bade['categort_id'] = bade.apply(lambda x: func(x.category_name), axis=1)
    # 删除_id,category_name列
    # bade = bade.drop(columns=['category_id'])  # 清洗pharmacy需要注释掉该行
    bade = bade.drop(columns=['_id', 'category_name'])
    # 按照品牌分组并进行分组排序取TOP50
    bade = bade.groupby(['brand_name']).apply(sort_data)
    # 根据pro_website去重
    bade = bade.drop_duplicates('pro_website', 'first')
    # 部分字段json序列化
    bade1 = bade[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_bade = pd.Series(bade1)
    df_bade = pd.DataFrame(sr_bade, columns=['product_atlas'])
    bade = bade.reset_index(drop=True)
    result = pd.concat([bade, df_bade], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    bade_pro = result.groupby(['brand_name']).apply(merge_js)
    bade_pro = pd.DataFrame(bade_pro, columns=['product_atlas'])
    bade_pro = bade_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    bade_data = pd.merge(bade_pro, result, how='inner', on=['brand_name'])
    bade_data = bade_data.drop(columns=['pro_pic'])

    # # 数据写入，可写入表格后利用mysql可视化工具导入到mysql数据库
    # bade_data.to_excel('C:/Users/Administrator/Desktop/{}badebuy_result.xls'.format(weekend), index=False)

    # 也可直接写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    db = pymysql.connect(host='192.168.2.199', user='root', password='root', db='d88_2.0_test', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    bade_list = bade_data.values.tolist()
    print(bade_list)
    print('写入数据库折扣信息数量为%d' % len(bade_list))
    for info in bade_list:
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
