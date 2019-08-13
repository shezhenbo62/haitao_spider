# coding:utf-8
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
    if cate == '奶粉' or cate == '纸尿裤/拉拉裤' or cate=='营养辅食' or cate=='宝宝用品/玩具图书' or cate=='童装童鞋' or cate=='孕妈必备':
        return 14
    elif cate == '乳品/咖啡/麦片/冲饮' or cate == '休闲零食' or cate == '茶/酒/饮料':
        return 8
    elif cate == '彩妆' or cate == '香水/香氛' or cate == '护肤' or cate=='面膜' or cate=='防晒修复':
        return 4
    elif cate == '洗发护发' or cate == '身体护理' or cate=='口腔护理' or cate=='女性护理' or cate=='成人健康':
        return 6
    elif cate == '潮品上新' or cate=='时尚型男' or cate=='精致女神':
        return 16


def run():
    # 抓下来的数据统一存入mongodb再做清洗工作
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    kaola = db['kaola']
    weekend = datetime.date.today()
    kaola = pd.DataFrame(list(kaola.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    # 增加categort_id列
    kaola['categort_id'] = kaola.apply(lambda x: func(x.category), axis=1)
    # 删除_id,category_name列
    kaola = kaola.drop(
        columns=['_id', 'activity', 'category', 'comment_count', 'comment_grade', 'sunburn_count', 'shop_name'])
    kaola = kaola.dropna(how='any', subset=['price', 'price_ref'])
    kaola['price'] = kaola['price'].astype('float64')
    kaola['price_ref'] = kaola['price_ref'].astype('float64')
    kaola['offers'] = kaola['price_ref'] - kaola['price']
    kaola['discount'] = round(kaola['price'] / kaola['price_ref'], 2)
    # 过滤掉折扣力度小的信息
    kaola = kaola.loc[kaola['discount'] <= 0.85]
    kaola = kaola.rename(
        columns={'brand': 'brand_name', 'goods_url': 'pro_website', 'img_url': 'pro_pic', 'price': 'pro_price_new',
                 'price_ref': 'pro_price_old', 'title': 'pro_title'})
    # 按照品牌分组并进行分组排序取TOP50
    kaola = kaola.groupby(['brand_name']).apply(sort_data)
    # 根据pro_website去重
    kaola = kaola.drop_duplicates('pro_website', 'first')
    # 部分字段json序列化
    kaola1 = kaola[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_kaola = pd.Series(kaola1)
    df_kaola = pd.DataFrame(sr_kaola, columns=['product_atlas'])
    kaola = kaola.reset_index(drop=True)
    result = pd.concat([kaola, df_kaola], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    kaola_pro = result.groupby(['brand_name']).apply(merge_js)
    kaola_pro = pd.DataFrame(kaola_pro, columns=['product_atlas'])
    kaola_pro = kaola_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    kaola_data = pd.merge(kaola_pro, result, how='inner', on=['brand_name'])
    kaola_data = kaola_data.drop(columns=['pro_pic'])

    # # 数据写入，可写入表格后利用mysql可视化工具导入到mysql数据库
    # kaola_data.to_excel('C:/Users/Administrator/Desktop/{}kaolabuy_result.xls'.format(weekend), index=False)

    # 也可直接写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    db = pymysql.connect(host='192.168.2.199', user='root', password='root', db='d88_2.0_test', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    kaola_list = kaola_data.values.tolist()
    print(kaola_list[:10])
    print('写入数据库折扣信息数量为%d' % len(kaola_list))
    for info in kaola_list:
        # 使用 execute()  方法执行 SQL
        sql = '''
        insert into d88_material (brand_name,product_atlas,create_time,url,current_price,
        original_price,describtion,category_id,discounts_money,discount)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        cursor.execute(sql, info)
        db.commit()
    db.close()


if __name__ == '__main__':
    run()
