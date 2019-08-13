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
    return group.sort_values(by=['expiration_time'], ascending=False)[:10]


def merge_js(group):
    return str(group.product_atlas.tolist())  # 列表数据类型转换为字符串，否则写入数据库会报错


def add_discount_info(x, y):
    return str([{"info": "活动", "pm": x, "id": ""},
                {"info": "现价", "pm": y, "id": ""}])


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


def change_create_time(x):
    weekend = datetime.date.today()
    zero_time = weekend.strftime('%Y-%m-%d %H:%M:%S')
    ran_hour = random.randint(0, 23)
    ran_second = random.randint(0, 3599)
    x['create_time'] = (datetime.datetime.strptime(zero_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(
        hours=ran_hour) + datetime.timedelta(seconds=ran_second)).strftime('%Y-%m-%d %H:%M:%S')
    return x


def get_result(jd):
    # 增加categort_id列
    jd = jd.apply(change_create_time, axis=1)
    jd["buy_source"] = "京东商城"
    jd["release_time"] = jd["create_time"]
    jd["expiration_time"] = pd.to_datetime(jd["create_time"]) + pd.Timedelta(days=3)
    jd["expiration_time"] = jd["expiration_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    jd["update_time"] = jd["create_time"]
    jd["status"] = 2
    jd['pro_price_new'] = jd['pro_price_new'].astype('float64')
    jd["pro_price_old"] = jd["pro_price_new"]

    # 按照品牌分组并进行分组排序取TOP10
    jd = jd.groupby(['brand_name']).apply(sort_data)
    # 根据pro_website去重
    jd = jd.drop_duplicates('pro_website', 'first')

    # 部分字段json序列化
    jd1 = jd[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_jd = pd.Series(jd1)
    df_jd = pd.DataFrame(sr_jd, columns=['product_atlas'])
    jd = jd.reset_index(drop=True)
    result = pd.concat([jd, df_jd], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    jd_pro = result.groupby(['brand_name']).apply(merge_js)
    jd_pro = pd.DataFrame(jd_pro, columns=['product_atlas'])
    jd_pro = jd_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    jd_data = pd.merge(jd_pro, result, how='inner', on=['brand_name'])

    # 增加discount_info列
    jd_data["discount_info"] = jd_data.apply(
        lambda col: add_discount_info(col.subheading, col.pro_price_new), axis=1)
    jd_result = jd_data.drop(columns=["brand_name", "pro_price_new", "pro_price_old", "_id"])
    return jd_result


def run():
    # 抓下来的数据统一存入mongodb再做清洗工作
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    jd = db['jd']
    weekend = datetime.date.today()
    jd = pd.DataFrame(list(jd.find(
        {'create_time': {'$regex': '^.*{}.*$'.format(weekend)}, '$or': [{'shop_name': {'$regex': '^.*自营|旗舰.*$'}}]})))
    jd = jd.rename(
        columns={'brand': 'brand_name', 'goods_url': 'pro_website', 'img_url': 'pro_pic', 'price': 'pro_price_new',
                 'title': 'pro_title', 'activity1': 'subheading'})
    jd = jd.dropna(how='all', subset=['subheading', 'activity2', 'activity6'])
    jd = jd.replace(["（", "）", "赠完即止", "条件：购买1件及以上，", "条件：购买2件及以上，",
                     "&nbsp;<a href='javascript:login\(\);'>请登录</a>&nbsp;确认是否享受优惠[0-9\u4e00-\u9fff，：]?"], "",
                    regex=True)
    jd['subheading'] = jd['subheading'].combine_first(jd['activity2'])
    jd = jd.loc[jd['subheading'].str.contains('减|折', na=False)]
    jd = jd.loc[~jd["subheading"].str.contains("购物车")]
    jd = jd.drop(columns=['activity2', 'activity6', 'comment_count', 'shop_name', 'activity_time'])
    jd["brand_name"] = jd["brand_name"].str.lower()
    jd_mongo = deepcopy(jd)
    print('新抓取数据量为%d条' % len(jd))
    # 增加categort_id列
    jd = jd.apply(change_create_time, axis=1)
    jd["buy_source"] = "京东商城"
    jd["release_time"] = jd["create_time"]
    jd["expiration_time"] = pd.to_datetime(jd["create_time"]) + pd.Timedelta(days=3)
    jd["expiration_time"] = jd["expiration_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    jd["update_time"] = jd["create_time"]
    jd["status"] = 2
    jd['pro_price_new'] = jd['pro_price_new'].astype('float64')
    jd["pro_price_old"] = jd["pro_price_new"]

    # 对品牌名进行名称规范一致性处理
    cn_brand_list = []
    en_brand_list = []
    for brand in jd[['_id', 'brand_name']].values.tolist():
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
    jd = pd.merge(result, jd, how='inner', on=['_id'])
    jd = jd.drop(columns=['_id'])

    # 按照品牌分组并进行分组排序取TOP10
    jd = jd.groupby(['brand_name']).apply(sort_data)
    # 根据pro_website去重
    jd = jd.drop_duplicates('pro_website', 'first')

    # 部分字段json序列化
    jd1 = jd[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_jd = pd.Series(jd1)
    df_jd = pd.DataFrame(sr_jd, columns=['product_atlas'])
    jd = jd.reset_index(drop=True)
    result = pd.concat([jd, df_jd], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    jd_pro = result.groupby(['brand_name']).apply(merge_js)
    jd_pro = pd.DataFrame(jd_pro, columns=['product_atlas'])
    jd_pro = jd_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    jd_data = pd.merge(jd_pro, result, how='inner', on=['brand_name'])

    # 增加discount_info列
    jd_data["discount_info"] = jd_data.apply(
        lambda col: add_discount_info(col.subheading, col.pro_price_new), axis=1)

    # 匹配brand_id
    jd_data = jd_data.apply(add_brand_id, axis=1)

    # 相似度小于0.9的数据排序后写入excel表格
    jd_data_brand = jd_data.loc[jd_data["similarity_degree"] < 0.9]
    jd_data_brand = jd_data_brand.sort_values(by="similarity_degree", ascending=False)
    jd_data_brand.to_excel('C:/Users/Administrator/Desktop/{}jd_result.xls'.format(weekend), index=False)

    # 相似度大于等于0.9的数据写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    jd_data_sql = jd_data.loc[jd_data["similarity_degree"] >= 0.9]
    jd_data_sql = jd_data_sql.drop(columns=["pro_price_new", "pro_price_old", "similarity_degree",
                                            "matching_brand_name", "brand_name_change"])
    print(len(jd_data_sql))
    jd_data_sql = pd.merge(jd_data_sql[['brand_name', 'brand_id']], jd_mongo, how='inner', on='brand_name')
    # 七牛云存储替换原图片链接
    p = Update_pic(jd_data_sql)
    jd_data_sql = p.replace_url()
    jd_result = get_result(jd_data_sql)
    jd_result = jd_result.dropna(how='any', subset=['pro_title', 'subheading'])
    # jd_result.to_excel('C:/Users/Administrator/Desktop/jd2233.xls')
    db = pymysql.connect(host='btadmin.d88.ink', user='worm_db', password='2TAHa6neRMwAeDGh', db='worm_db', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    jd_list = jd_result.values.tolist()
    print('写入数据库折扣信息数量为%d' % len(jd_list))
    for info in jd_list:
        info[0] = info[0].replace("'", '"')
        info[-1] = info[-1].replace("'", '"')
        # 使用 execute()  方法执行 SQL
        sql = """
        insert into d88_discount_reptile (product_atlas,brand_id,subheading,category_id,create_time,
        buy_source_url,cover_img,title,buy_source,release_time,expiration_time,update_time,status,discount_info)
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
