# coding:utf-8
import pandas as pd
import Levenshtein
import pymongo
import datetime
import pymysql
import random
from pd_handle_discount_reptile.qiniu_test import Update_pic
from copy import deepcopy

"""
数据清洗及写入mysql数据库，前端折扣数据填充
"""


def sort_data(group):
    return group.sort_values(by=["offers", "discount"], ascending=False)[:10]


def merge_js(group):
    return str(group.product_atlas.tolist())  # 列表数据类型转换为字符串，否则写入数据库会报错


def add_discount_info(x, y, z):
    return str([{"info": "折扣", "pm": str(round(x*10, 1))+"折", "id": ""}, {"info": "现价", "pm": y, "id": ""}, {"info": "原价", "pm": z, "id": ""}])


def add_subheading(s, m):
    activity = ["商品直降¥"+str(s).split(".")[0],
                str(round(m*10, 1)) + "折优惠",
                "商品享" + str(round(m * 10, 1)) + "折"]
    return random.choice(activity)


def add_brand_id(s):
    brand_list = database_brand.values.tolist()
    diff_list = []
    for word in brand_list:
        score = Levenshtein.ratio(s["brand_name"], str(word[2]))
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


def get_result(yoho):
    # 增加分类id,购买来源,活动开始时间，过期时间，更新时间，折扣状态
    # 3为服饰类别
    yoho = yoho.apply(change_create_time, axis=1)
    yoho["categort_id"] = 16
    yoho["buy_source"] = "yohobuy"
    yoho["release_time"] = yoho["create_time"]
    yoho["expiration_time"] = pd.to_datetime(yoho["create_time"]) + pd.Timedelta(days=3)
    yoho["expiration_time"] = yoho["expiration_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    yoho["update_time"] = yoho["create_time"]
    yoho["status"] = 2
    yoho["subheading"] = yoho.apply(lambda x: add_subheading(x.offers, x.discount), axis=1)
    # 删除_id列
    yoho = yoho.drop(columns=["_id"])
    # 清洗无关字符
    yoho = yoho.replace(r"\\n", "", regex=True)
    yoho = yoho.replace("¥", "", regex=True)
    # url绝对化
    yoho["pro_website"] = "https:" + yoho.pro_website
    # 过滤掉折扣力度小的信息
    yoho = yoho.loc[yoho["discount"] != 1]
    # 品牌名降为小写字母
    yoho["brand_name"] = yoho["brand_name"].str.lower()
    # 按照品牌分组并进行分组排序取TOP10
    yoho = yoho.groupby(["brand_name"]).apply(sort_data)
    # 根据pro_website去重
    yoho = yoho.drop_duplicates("pro_website", "first")

    # 部分字段json序列化
    yoho1 = yoho[["pro_title", "pro_website", "pro_pic", "pro_price_new", "pro_price_old"]].to_dict("records")
    sr_yoho = pd.Series(yoho1)
    df_yoho = pd.DataFrame(sr_yoho, columns=["product_atlas"])
    yoho = yoho.reset_index(drop=True)
    result = pd.concat([yoho, df_yoho], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    yoho_pro = result.groupby(["brand_name"]).apply(merge_js)
    yoho_pro = pd.DataFrame(yoho_pro, columns=["product_atlas"])
    yoho_pro = yoho_pro.reset_index()
    result = result.drop_duplicates("brand_name", "first")
    result.drop(["product_atlas"], axis=1, inplace=True)
    yohobuy_data = pd.merge(yoho_pro, result, how="inner", on=["brand_name"])

    # 增加discount_info列
    yohobuy_data["discount_info"] = yohobuy_data.apply(
        lambda col: add_discount_info(col.discount, col.pro_price_new, col.pro_price_old), axis=1)
    yoho_result = yohobuy_data.drop(columns=["brand_name", "discount", "offers", "pro_price_new", "pro_price_old"])
    return yoho_result


def run():
    # 抓下来的数据统一存入mongodb再做清洗工作
    client = pymongo.MongoClient("localhost", 27017)
    db = client["haitao"]
    yoho = db["yoho"]
    weekend = datetime.date.today()
    yoho = pd.DataFrame(list(yoho.find({"create_time": {"$regex": "^.*{}.*$".format(weekend)}})))
    yoho_mongo = deepcopy(yoho)
    yoho_mongo["brand_name"] = yoho_mongo["brand_name"].str.lower()
    print('新抓取数据量为%d条' % len(yoho))
    # 增加分类id,购买来源,活动开始时间，过期时间，更新时间，折扣状态
    # 3为服饰类别
    yoho = yoho.apply(change_create_time, axis=1)
    yoho["categort_id"] = 16
    yoho["buy_source"] = "yohobuy"
    yoho["release_time"] = yoho["create_time"]
    yoho["expiration_time"] = pd.to_datetime(yoho["create_time"]) + pd.Timedelta(days=3)
    yoho["expiration_time"] = yoho["expiration_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    yoho["update_time"] = yoho["create_time"]
    yoho["status"] = 2
    yoho["subheading"] = yoho.apply(lambda x: add_subheading(x.offers, x.discount), axis=1)
    # 删除_id列
    yoho = yoho.drop(columns=["_id"])
    # 清洗无关字符
    yoho = yoho.replace(r"\\n", "", regex=True)
    yoho = yoho.replace("¥", "", regex=True)
    # url绝对化
    yoho["pro_website"] = "https:" + yoho.pro_website
    # 过滤掉折扣力度小的信息
    yoho = yoho.loc[yoho["discount"] != 1]
    # 品牌名降为小写字母
    yoho["brand_name"] = yoho["brand_name"].str.lower()
    # 按照品牌分组并进行分组排序取TOP10
    yoho = yoho.groupby(["brand_name"]).apply(sort_data)
    # 根据pro_website去重
    yoho = yoho.drop_duplicates("pro_website", "first")
    
    # 部分字段json序列化
    yoho1 = yoho[["pro_title", "pro_website", "pro_pic", "pro_price_new", "pro_price_old"]].to_dict("records")
    sr_yoho = pd.Series(yoho1)
    df_yoho = pd.DataFrame(sr_yoho, columns=["product_atlas"])
    yoho = yoho.reset_index(drop=True)
    result = pd.concat([yoho, df_yoho], axis=1)

    # 同品牌数据 json字符串字段 并到列表中
    yoho_pro = result.groupby(["brand_name"]).apply(merge_js)
    yoho_pro = pd.DataFrame(yoho_pro, columns=["product_atlas"])
    yoho_pro = yoho_pro.reset_index()
    result = result.drop_duplicates("brand_name", "first")
    result.drop(["product_atlas"], axis=1, inplace=True)
    yohobuy_data = pd.merge(yoho_pro, result, how="inner", on=["brand_name"])

    # 增加discount_info列
    yohobuy_data["discount_info"] = yohobuy_data.apply(
        lambda col: add_discount_info(col.discount, col.pro_price_new, col.pro_price_old), axis=1)

    # 匹配brand_id
    yohobuy_data = yohobuy_data.apply(add_brand_id, axis=1)

    # 相似度小于0.9的数据排序后写入excel表格
    yohobuy_data_brand = yohobuy_data.loc[yohobuy_data["similarity_degree"] < 0.9]
    yohobuy_data_brand = yohobuy_data_brand.sort_values(by="similarity_degree", ascending=False)
    yohobuy_data_brand.to_excel("C:/Users/Administrator/Desktop/{}yohobuy_result.xls".format(weekend), index=False)

    # 相似度大于等于0.9的数据写入对应的mysql数据库，注意字段对应，不同爬虫执行此代码sql语句可能需要改写
    yohobuy_data_sql = yohobuy_data.loc[yohobuy_data["similarity_degree"] >= 0.9]
    yohobuy_data_sql = yohobuy_data_sql.drop(columns=["discount", "offers", "pro_price_new",
                                                      "pro_price_old", "similarity_degree", "matching_brand_name"])
    print(len(yohobuy_data_sql))
    yohobuy_data_sql = pd.merge(yohobuy_data_sql[['brand_name', 'brand_id']], yoho_mongo, how='inner', on='brand_name')

    # 七牛云存储替换原图片链接
    p = Update_pic(yohobuy_data_sql)
    yohobuy_data_sql = p.replace_url()
    yoho_result = get_result(yohobuy_data_sql)
    db = pymysql.connect(host='btadmin.d88.ink', user='worm_db', password='2TAHa6neRMwAeDGh', db='worm_db', port=3306)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    yohobuy_list = yoho_result.values.tolist()
    print('写入数据库折扣信息数量为%d' % len(yohobuy_list))
    for info in yohobuy_list:
        info[0] = info[0].replace("'", '"')
        info[-1] = info[-1].replace("'", '"')
        # 使用 execute()  方法执行 SQL
        sql = """
        insert into d88_discount_reptile (product_atlas,brand_id,create_time,cover_img,title,
        buy_source_url,category_id,buy_source,release_time,expiration_time,update_time,status,subheading,discount_info)
        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        cursor.execute(sql, info)
        db.commit()
    db.close()


if __name__ == "__main__":
    client = pymongo.MongoClient("localhost", 27017)
    db = client["haitao"]
    brand = db["D88_brand"]
    database_brand = pd.DataFrame(list(brand.find({})))
    database_brand = database_brand.drop(columns=["_id"])
    run()
