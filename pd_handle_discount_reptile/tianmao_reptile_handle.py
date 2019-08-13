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
    return group.sort_values(by=['discount'], ascending=True)[:10]


def merge_js(group):
    return str(group.product_atlas.tolist())  # 列表数据类型转换为字符串，否则写入数据库会报错


def func(cate):
    if cate == '母婴':
        return 14
    elif cate == '保健' or cate == '零食' or cate == '酒水':
        return 8
    elif cate == '美妆':
        return 4
    elif cate == '行李箱包' or cate == '服饰' or cate == '高端服饰':
        return 16
    elif cate == '杂货清洁' or cate == '家居' or cate == '厨房家电':
        return 6
    elif cate == '电子':
        return 5


def add_discount_info(x, y, z):
    return str([{"info": "活动", "pm": x, "id": ""},
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


def add_subheading(s, m):
    if s:
        activity_list = ["商品直降¥"+str(s),
                    "商品享" + str(round(m * 10, 1)) + "折",
                    str(round(m * 10, 1)) + "折优惠"]
        activity = random.choice(activity_list)
        return activity
    else:
        return s


def change_create_time(x):
    weekend = datetime.date.today()
    zero_time = weekend.strftime('%Y-%m-%d %H:%M:%S')
    ran_hour = random.randint(0, 23)
    ran_second = random.randint(0, 3599)
    x['create_time'] = (datetime.datetime.strptime(zero_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(
        hours=ran_hour) + datetime.timedelta(seconds=ran_second)).strftime('%Y-%m-%d %H:%M:%S')
    return x


def get_result(tianmao):
    # 增加category_id列
    tianmao = tianmao.apply(change_create_time, axis=1)
    tianmao['category_id'] = tianmao.apply(lambda x: func(x.category), axis=1)
    tianmao["buy_source"] = "天猫商城"
    tianmao["release_time"] = tianmao["create_time"]
    tianmao["expiration_time"] = pd.to_datetime(tianmao["create_time"]) + pd.Timedelta(days=3)
    tianmao["expiration_time"] = tianmao["expiration_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    tianmao["update_time"] = tianmao["create_time"]
    tianmao["status"] = 2
    tianmao['pro_price_new'] = tianmao['pro_price_new'].astype('float64')
    tianmao["pro_price_old"] = tianmao["pro_price_old"].astype('float64')
    tianmao["offers"] = round(tianmao["pro_price_old"] - tianmao["pro_price_new"], 2)
    tianmao["discount"] = round(tianmao["pro_price_new"] / tianmao["pro_price_old"], 2)
    tianmao["offers"] = tianmao["offers"].where(tianmao["offers"] > 0, np.nan)
    tianmao = tianmao.dropna(how='all', subset=['subheading', 'offers'])
    tianmao["offers"] = tianmao.apply(lambda x: add_subheading(x.offers, x.discount), axis=1)
    tianmao['subheading'] = tianmao['subheading'].combine_first(tianmao['offers'])

    # 按照品牌分组并进行分组排序取TOP10
    tianmao = tianmao.groupby(['brand_name']).apply(sort_data)
    # 根据pro_website去重
    tianmao = tianmao.drop_duplicates('pro_website', 'first')

    # 部分字段json序列化
    tianmao1 = tianmao[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_tianmao = pd.Series(tianmao1)
    df_tianmao = pd.DataFrame(sr_tianmao, columns=['product_atlas'])
    tianmao = tianmao.reset_index(drop=True)
    result = pd.concat([tianmao, df_tianmao], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    tianmao_pro = result.groupby(['brand_name']).apply(merge_js)
    tianmao_pro = pd.DataFrame(tianmao_pro, columns=['product_atlas'])
    tianmao_pro = tianmao_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    tianmao_data = pd.merge(tianmao_pro, result, how='inner', on=['brand_name'])

    # 增加discount_info列
    tianmao_data["discount_info"] = tianmao_data.apply(
        lambda col: add_discount_info(col.subheading, col.pro_price_new, col.pro_price_old), axis=1)
    tianmao_result = tianmao_data.drop(columns=["_id", "brand_name", "discount", "offers", "pro_price_new", "category",
                                                "pro_price_old"])
    return tianmao_result


def run():
    # 抓下来的数据统一存入mongodb再做清洗工作
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    tianmao = db['tianmao']
    weekend = datetime.date.today()
    tianmao = pd.DataFrame(list(tianmao.find(
        {'create_time': {'$regex': '^.*{}.*$'.format(weekend)}, '$or': [{'shop_name': {'$regex': '^.*直营|旗舰.*$'}}]})))
    tianmao["brand_name"] = tianmao["brand_name"].str.lower()
    tianmao["brand_name"] = tianmao["brand_name"].replace(["（", "）", "/", "！", "-", "·"], "", regex=True)
    tianmao = tianmao.rename(columns={'activity': 'subheading'})
    tianmao["subheading"] = tianmao["subheading"].replace('"', '', regex=True)
    tianmao = tianmao.drop(columns=['comment_count', 'phone_goods_url', 'sale_count', 'shop_name'])
    tianmao_mongo = deepcopy(tianmao)
    print('新抓取数据量为%d条' % len(tianmao))
    # 增加categort_id列
    tianmao = tianmao.apply(change_create_time, axis=1)
    tianmao['category_id'] = tianmao.apply(lambda x: func(x.category), axis=1)
    tianmao["buy_source"] = "天猫商城"
    tianmao["release_time"] = tianmao["create_time"]
    tianmao["expiration_time"] = pd.to_datetime(tianmao["create_time"]) + pd.Timedelta(days=3)
    tianmao["expiration_time"] = tianmao["expiration_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    tianmao["update_time"] = tianmao["create_time"]
    tianmao["status"] = 2
    tianmao['pro_price_new'] = tianmao['pro_price_new'].astype('float64')
    tianmao["pro_price_old"] = tianmao["pro_price_old"].astype('float64')
    tianmao["offers"] = round(tianmao["pro_price_old"] - tianmao["pro_price_new"], 2)
    tianmao["discount"] = round(tianmao["pro_price_new"] / tianmao["pro_price_old"], 2)
    tianmao["offers"] = tianmao["offers"].where(tianmao["offers"] > 0, np.nan)
    tianmao = tianmao.dropna(how='all', subset=['subheading', 'offers'])
    tianmao["offers"] = tianmao.apply(lambda x: add_subheading(x.offers, x.discount), axis=1)
    tianmao['subheading'] = tianmao['subheading'].combine_first(tianmao['offers'])

    # 对品牌名进行名称规范一致性处理
    cn_brand_list = []
    en_brand_list = []
    for brand in tianmao[['_id', 'brand_name']].values.tolist():
        split_list = []
        if all(['\u4e00' <= i <= '\u9fff' for i in str(brand[1])]):
            cn_brand_list.append([brand[0], brand[1].lower()])
        elif all(['a' <= j <= 'z' for j in str(brand[1])]):
            en_brand_list.append([brand[0], brand[1].lower()])
        else:
            for j in str(brand[1]):
                if not ('\u4e00' <= j <= '\u9fff'):
                    split_list.append(j)
            merge_str = ''.join(split_list)
            merge_str = merge_str.strip()
            en_brand_list.append([brand[0], merge_str.lower()])
    all_list = en_brand_list + cn_brand_list
    result = pd.DataFrame(all_list, columns=['_id', 'brand_name_change'])
    tianmao = pd.merge(result, tianmao, how='inner', on=['_id'])
    tianmao = tianmao.drop(columns=['_id'])

    # 按照品牌分组并进行分组排序取TOP10
    tianmao = tianmao.groupby(['brand_name']).apply(sort_data)
    # 根据pro_website去重
    tianmao = tianmao.drop_duplicates('pro_website', 'first')

    # 部分字段json序列化
    tianmao1 = tianmao[['pro_title', 'pro_website', 'pro_pic', 'pro_price_new', 'pro_price_old']].to_dict('records')
    sr_tianmao = pd.Series(tianmao1)
    df_tianmao = pd.DataFrame(sr_tianmao, columns=['product_atlas'])
    tianmao = tianmao.reset_index(drop=True)
    result = pd.concat([tianmao, df_tianmao], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    tianmao_pro = result.groupby(['brand_name']).apply(merge_js)
    tianmao_pro = pd.DataFrame(tianmao_pro, columns=['product_atlas'])
    tianmao_pro = tianmao_pro.reset_index()
    result = result.drop_duplicates('brand_name', 'first')
    result.drop(['product_atlas'], axis=1, inplace=True)
    tianmao_data = pd.merge(tianmao_pro, result, how='inner', on=['brand_name'])

    # 增加discount_info列
    tianmao_data["discount_info"] = tianmao_data.apply(
        lambda col: add_discount_info(col.subheading, col.pro_price_new, col.pro_price_old), axis=1)

    # 匹配brand_id
    tianmao_data = tianmao_data.apply(add_brand_id, axis=1)

    # 相似度小于0.9的数据排序后写入excel表格
    tianmao_data_brand = tianmao_data.loc[tianmao_data["similarity_degree"] < 0.9]
    tianmao_data_brand = tianmao_data_brand.sort_values(by="similarity_degree", ascending=False)
    tianmao_data_brand.to_excel('C:/Users/Administrator/Desktop/{}tianmao_result.xls'.format(weekend), index=False)

    # 相似度大于等于0.9的数据写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    tianmao_data_sql = tianmao_data.loc[tianmao_data["similarity_degree"] >= 0.9]
    tianmao_data_sql = tianmao_data_sql.drop(columns=["discount", "offers", "pro_price_new", "category",
                                                      "pro_price_old", "similarity_degree", "matching_brand_name",
                                                      "brand_name_change"])
    print(len(tianmao_data_sql))
    tianmao_data_sql = pd.merge(tianmao_data_sql[['brand_name', 'brand_id']], tianmao_mongo, how='inner',
                                on='brand_name')
    # 七牛云存储替换原图片链接
    p = Update_pic(tianmao_data_sql)
    tianmao_data_sql = p.replace_url()
    tianmao_result = get_result(tianmao_data_sql)
    tianmao_result = tianmao_result.dropna(how='any', subset=['subheading'])
    # tianmao_result.to_excel('C:/Users/Administrator/Desktop/tianmao2233.xls')
    db = pymysql.connect(host='btadmin.d88.ink', user='worm_db', password='2TAHa6neRMwAeDGh', db='worm_db', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    tianmao_list = tianmao_result.values.tolist()
    print('写入数据库折扣信息数量为%d' % len(tianmao_list))
    for info in tianmao_list:
        info[0] = info[0].replace("'", '"')
        info[-1] = info[-1].replace("'", '"')
        # 使用 execute()  方法执行 SQL
        sql = """
        insert into d88_discount_reptile (product_atlas,brand_id,subheading,create_time,cover_img,title,
            buy_source_url,category_id,buy_source,release_time,expiration_time,update_time,status,discount_info)
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
