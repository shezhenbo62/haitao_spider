# coding:utf-8
import pandas as pd
import pymongo
import datetime


def run():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']

    # TODO 每个网站更换读取的mongodb数据库集合名
    # bade = db['bade']
    # bade = db['farfetch']
    # bade = db['pharmacy']
    # bade = db['self']
    # bade = db['unineed']
    # bade = db['yoho']
    # bade = db['kaola']
    bade = db['jd']

    weekend = datetime.date.today()
    bade = pd.DataFrame(list(bade.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    bade = bade.drop(columns=['_id'])
    bade = bade.rename(columns={'brand': 'brand_name'})
    bade["brand_name"] = bade["brand_name"].str.lower()
    bade = bade.replace(["（", "）"], "", regex=True)  # 京东须打开注释

    # TODO 每个网站需要更换读取的 名称，以及要读取的品牌名brand_name和category_id的序号
    # df = pd.read_excel('C:/Users/Administrator/Desktop/{}bade_result.xls'.format(weekend), usecols=[0, 10])
    # df = pd.read_excel('C:/Users/Administrator/Desktop/{}farfetch_result.xls'.format(weekend), usecols=[0, 10])
    # df = pd.read_excel('C:/Users/Administrator/Desktop/{}pharmacy_result.xls'.format(weekend), usecols=[0, 10])
    # df = pd.read_excel('C:/Users/Administrator/Desktop/{}selfridges_result.xls'.format(weekend), usecols=[0, 10])
    # df = pd.read_excel('C:/Users/Administrator/Desktop/{}unineed_result.xls'.format(weekend), usecols=[0, 10])
    # df = pd.read_excel('C:/Users/Administrator/Desktop/{}yohobuy_result.xls'.format(weekend), usecols=[0, 10])
    # df = pd.read_excel('C:/Users/Administrator/Desktop/{}kaola_result.xls'.format(weekend), usecols=[0, 10])
    df = pd.read_excel('C:/Users/Administrator/Desktop/{}jd_result.xls'.format(weekend), usecols=[0])

    result = pd.merge(df, bade, how='inner', on='brand_name')

    # TODO 写入文件名按照对应网站名称修改
    # result.to_excel('C:/Users/Administrator/Desktop/{}bade_to_editor.xls'.format(weekend), index=False)
    # result.to_excel('C:/Users/Administrator/Desktop/{}farfetch_to_editor.xls'.format(weekend), index=False)
    # result.to_excel('C:/Users/Administrator/Desktop/{}pharmacy_to_editor.xls'.format(weekend), index=False)
    # result.to_excel('C:/Users/Administrator/Desktop/{}selfridges_to_editor.xls'.format(weekend), index=False)
    # result.to_excel('C:/Users/Administrator/Desktop/{}unineed_to_editor.xls'.format(weekend), index=False)
    # result.to_excel('C:/Users/Administrator/Desktop/{}yohobuy_to_editor.xls'.format(weekend), index=False)
    # result.to_excel('C:/Users/Administrator/Desktop/{}kaola_to_editor.xls'.format(weekend), index=False)
    result.to_excel('C:/Users/Administrator/Desktop/{}jd_to_editor.xls'.format(weekend), index=False)


if __name__ == '__main__':
    run()
