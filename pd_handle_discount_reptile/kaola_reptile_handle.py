# coding:utf-8
import pandas as pd
import Levenshtein
import pymongo
import datetime
import pymysql
import random
from pd_handle_discount_reptile.qiniu_test import Update_pic
from copy import deepcopy
import numpy as np

"""
数据清洗及写入mysql数据库
"""


def sort_data(group):
    return group.sort_values(by=['offers', 'discount'], ascending=False)[:10]


def merge_js(group):
    return str(group.product_atlas.tolist())  # 列表数据类型转换为字符串，否则写入数据库会报错


def func(cate):
    if cate == '奶粉' or cate == '纸尿裤/拉拉裤' or cate == '营养辅食' or cate == '宝宝用品/玩具图书' or cate == '童装童鞋' or cate == '孕妈必备':
        return 14
    elif cate == '乳品/咖啡/麦片/冲饮' or cate == '休闲零食' or cate == '茶/酒/饮料':
        return 8
    elif cate == '彩妆' or cate == '香水/香氛' or cate == '护肤' or cate == '面膜' or cate == '防晒修复':
        return 4
    elif cate == '洗发护发' or cate == '身体护理' or cate == '口腔护理' or cate == '女性护理' or cate == '成人健康':
        return 10
    elif cate == '潮品上新' or cate == '时尚型男' or cate == '精致女神':
        return 16
    elif cate == '杯壶滤水' or cate == '厨房餐具' or cate == '家纺布艺' or cate == '家具家装' or cate == '家居清洁' or cate == '日用百货':
        return 6


def add_discount_info(s, x, y, z):
    return str([{"info": "活动", "pm": " ".join(s.split()), "id": ""},
                {"info": "折扣", "pm": str(round(x*10, 1))+"折", "id": ""},
                {"info": "现价", "pm": y, "id": ""},
                {"info": "原价", "pm": z, "id": ""}])


def add_brand_id(s):
    brand_list = database_brand.values.tolist()
    diff_list = []
    for word in brand_list:
        score = Levenshtein.ratio(s["brand_name_change"], str(word[2]))
        diff_list.append([score, word[0], word[1]])
    diff_list.sort(reverse=True, key=lambda y: y[0])
    s["similarity_degree"] = diff_list[0][0]
    s["brand_id"] = diff_list[0][1]
    s["matching_brand_name"] = diff_list[0][2]
    return s


def add_subheading(s, m, z):
    activity_list = ["商品直降¥"+str(s).split(".")[0],
                "商品享" + str(round(m * 10, 1)) + "折",
                str(round(m * 10, 1)) + "折优惠",
                " ".join(z.split())]
    activity = random.choice(activity_list)
    if activity:
        return activity
    else:
        return np.nan


def change_create_time(x):
    weekend = datetime.date.today()
    zero_time = weekend.strftime('%Y-%m-%d %H:%M:%S')
    ran_hour = random.randint(0, 23)
    ran_second = random.randint(0, 3599)
    x['create_time'] = (datetime.datetime.strptime(zero_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(
        hours=ran_hour) + datetime.timedelta(seconds=ran_second)).strftime('%Y-%m-%d %H:%M:%S')
    return x


def get_result(kaola):
    # 增加categort_id列
    kaola = kaola.apply(change_create_time, axis=1)
    kaola['categort_id'] = kaola.apply(lambda x: func(x.category), axis=1)
    kaola["buy_source"] = "考拉自营"
    kaola["release_time"] = kaola["create_time"]
    kaola["expiration_time"] = pd.to_datetime(kaola["create_time"]) + pd.Timedelta(days=3)
    kaola["expiration_time"] = kaola["expiration_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    kaola["update_time"] = kaola["create_time"]
    kaola["status"] = 2
    # 删除_id,category_name列
    kaola = kaola.drop(
        columns=['_id', 'brand', 'category', 'comment_count', 'comment_grade', 'sunburn_count', 'shop_name'])
    kaola = kaola.dropna(how='any', subset=['price', 'price_ref', 'activity'])
    kaola['price'] = kaola['price'].astype('float64')
    kaola['price_ref'] = kaola['price_ref'].astype('float64')
    kaola['offers'] = kaola['price_ref'] - kaola['price']
    kaola['discount'] = round(kaola['price'] / kaola['price_ref'], 2)
    kaola["subheading"] = kaola.apply(lambda x: add_subheading(x.offers, x.discount, x.activity), axis=1)
    # 过滤掉折扣力度小的信息
    kaola = kaola.loc[kaola['discount'] < 1]
    kaola = kaola.rename(
        columns={'goods_url': 'pro_website', 'img_url': 'pro_pic', 'price': 'pro_price_new',
                 'price_ref': 'pro_price_old', 'title': 'pro_title'})
    # 按照品牌分组并进行分组排序取TOP10
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

    # 增加discount_info列
    kaola_data["discount_info"] = kaola_data.apply(
        lambda col: add_discount_info(col.activity, col.discount, col.pro_price_new, col.pro_price_old), axis=1)
    kaola_result = kaola_data.drop(columns=["brand_name", "discount", "offers", "pro_price_new", "activity", "pro_price_old"])
    return kaola_result


def run():
    # 抓下来的数据统一存入mongodb再做清洗工作
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    kaola = db['kaola']
    weekend = datetime.date.today()
    kaola = pd.DataFrame(list(kaola.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    kaola = kaola.dropna(how='any', subset=['activity'])
    kaola["brand"] = kaola["brand"].str.lower()
    kaola_mongo = deepcopy(kaola)
    print('新抓取数据量为%d条' % len(kaola))
    # 增加categort_id列
    kaola = kaola.apply(change_create_time, axis=1)
    kaola['categort_id'] = kaola.apply(lambda x: func(x.category), axis=1)
    kaola["buy_source"] = "考拉自营"
    kaola["release_time"] = kaola["create_time"]
    kaola["expiration_time"] = pd.to_datetime(kaola["create_time"]) + pd.Timedelta(days=3)
    kaola["expiration_time"] = kaola["expiration_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    kaola["update_time"] = kaola["create_time"]
    kaola["status"] = 2
    # 删除_id,category_name列
    kaola = kaola.drop(
        columns=['category', 'comment_count', 'comment_grade', 'sunburn_count', 'shop_name'])
    kaola = kaola.dropna(how='any', subset=['price', 'price_ref', 'activity'])
    kaola['price'] = kaola['price'].astype('float64')
    kaola['price_ref'] = kaola['price_ref'].astype('float64')
    kaola['offers'] = kaola['price_ref'] - kaola['price']
    kaola['discount'] = round(kaola['price'] / kaola['price_ref'], 2)
    kaola["subheading"] = kaola.apply(lambda x: add_subheading(x.offers, x.discount, x.activity), axis=1)
    # 过滤掉折扣力度小的信息
    kaola = kaola.loc[kaola['discount'] < 1]
    kaola = kaola.rename(
        columns={'brand': 'brand_name', 'goods_url': 'pro_website', 'img_url': 'pro_pic', 'price': 'pro_price_new',
                 'price_ref': 'pro_price_old', 'title': 'pro_title'})

    # 对品牌名进行名称规范一致性处理
    cn_brand_list = []
    en_brand_list = []
    for brand in kaola[['_id', 'brand_name']].values.tolist():
        split_list = []
        if all(['\u4e00' <= i <= '\u9fff' for i in str(brand[1])]):
            cn_brand_list.append([brand[0], brand[1].lower()])
        elif all(['a' <= j <= 'z' for j in str(brand[1])]):
            en_brand_list.append([brand[0], brand[1].lower()])
        else:
            for j in str(brand[1]):
                if not ('\u4e00' <= j <= '\u9fff' or j == '/' or j == '·'):
                    split_list.append(j)
            merge_str = ''.join(split_list)
            merge_str = merge_str.strip()
            en_brand_list.append([brand[0], merge_str.lower()])
    all_list = en_brand_list + cn_brand_list
    result = pd.DataFrame(all_list, columns=['_id', 'brand_name_change'])
    kaola = pd.merge(result, kaola, how='inner', on=['_id'])
    kaola = kaola.drop(columns=['_id'])

    # 按照品牌分组并进行分组排序取TOP10
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

    # 增加discount_info列
    kaola_data["discount_info"] = kaola_data.apply(
        lambda col: add_discount_info(col.activity, col.discount, col.pro_price_new, col.pro_price_old), axis=1)

    # 匹配brand_id
    kaola_data = kaola_data.apply(add_brand_id, axis=1)

    # 相似度小于0.9的数据排序后写入excel表格
    kaola_data_brand = kaola_data.loc[kaola_data["similarity_degree"] < 0.9]
    kaola_data_brand = kaola_data_brand.sort_values(by="similarity_degree", ascending=False)
    kaola_data_brand.to_excel('C:/Users/Administrator/Desktop/{}kaola_result.xls'.format(weekend), index=False)

    # 相似度大于等于0.9的数据写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    kaola_data_sql = kaola_data.loc[kaola_data["similarity_degree"] >= 0.9]
    kaola_data_sql = kaola_data_sql.drop(columns=["discount", "offers", "pro_price_new", "activity",
                                                  "pro_price_old", "similarity_degree", "matching_brand_name",
                                                  "brand_name_change"])
    print(len(kaola_data_sql))
    kaola_data_sql = pd.merge(kaola_data_sql[['brand_name', 'brand_id']], kaola_mongo, how='inner',
                              left_on='brand_name', right_on='brand')
    kaola_data_sql = kaola_data_sql.rename(columns={'img_url': 'pro_pic'})
    # 七牛云存储替换原图片链接
    p = Update_pic(kaola_data_sql)
    kaola_data_sql = p.replace_url()
    kaola_result = get_result(kaola_data_sql)
    kaola_result = kaola_result.dropna(how='any', subset=['subheading'])
    # kaola_result.to_excel('C:/Users/Administrator/Desktop/kaola2233.xls')
    db = pymysql.connect(host='btadmin.d88.ink', user='worm_db', password='2TAHa6neRMwAeDGh', db='worm_db', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    kaola_list = kaola_result.values.tolist()
    print('写入数据库折扣信息数量为%d' % len(kaola_list))
    for info in kaola_list:
        info[0] = info[0].replace("'", '"')
        info[-1] = info[-1].replace("'", '"')
        # 使用 execute()  方法执行 SQL
        sql = """
        insert into d88_discount_reptile (product_atlas,brand_id,create_time,buy_source_url,cover_img,title,category_id,
        buy_source,release_time,expiration_time,update_time,status,subheading,discount_info)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql, info)
        db.commit()
    db.close()


if __name__ == '__main__':
    client = pymongo.MongoClient("localhost", 27017)
    db = client["haitao"]
    brand = db["D88_brand"]
    database_brand = pd.DataFrame(list(brand.find({})))
    database_brand = database_brand.drop(columns=["_id"])
    run()
