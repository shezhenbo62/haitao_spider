# coding:utf-8

import pandas as pd
import pymongo
import datetime

pd.set_option('display.max_columns', None)
# pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 100)
pd.set_option('display.expand_frame_rep', False)


def sort_data(group):
    return group.sort_values(by=['offers', 'discount'], ascending=False)[:50]


def get_top50(group):
    return group.sort_values(by=['offers', 'comment_count'], ascending=False)[:50]


def get_tmTop50(group):
    return group.sort_values(by=['offers', 'sale_count'], ascending=False)[:50]


def parse_pharmacy():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    pharmacy = db['pharmacy']
    weekend = datetime.date.today()
    data = pd.DataFrame(list(pharmacy.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    data = data.drop(columns=['_id'])
    order = ['category_name', 'brand_name', 'pro_title', 'pro_website', 'pro_pic',
             'pro_price_new', 'pro_price_old', 'offers', 'discount', 'create_time']
    data = data[order]
    data = data.groupby(['category_name', 'brand_name']).apply(sort_data)
    a = data.loc[data['category_name'] == '母婴产品']
    b = data.loc[data['category_name'] == '营养保健']
    c = data.loc[data['category_name'] == '美妆个护']
    d = data.loc[data['category_name'] == '食品饮料']
    e = data.loc[data['category_name'] == '洗护日用']
    writer = pd.ExcelWriter("C:/Users/Administrator/Desktop/{}pharmacy.xls".format(weekend))
    a.to_excel(excel_writer=writer, sheet_name='母婴产品')
    b.to_excel(excel_writer=writer, sheet_name='营养保健')
    c.to_excel(excel_writer=writer, sheet_name='美妆个护')
    d.to_excel(excel_writer=writer, sheet_name='食品饮料')
    e.to_excel(excel_writer=writer, sheet_name='洗护日用')
    writer.save()
    writer.close()


def parse_bade():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    bade = db['bade']
    weekend = datetime.date.today()
    data = pd.DataFrame(list(bade.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    data = data.drop(columns=['_id'])
    brand_drop = data.groupby(['category_name', 'brand_name']).apply(sort_data)
    # brand_drop = brand_drop.loc[brand_drop['discount'] <= 0.85]
    a = brand_drop.loc[brand_drop['category_name']=='母婴世界']
    b = brand_drop.loc[brand_drop['category_name']=='营养保健']
    c = brand_drop.loc[brand_drop['category_name']=='个护美妆']
    d = brand_drop.loc[brand_drop['category_name']=='环球美食']
    e = brand_drop.loc[brand_drop['category_name']=='居家日用']
    f = brand_drop.loc[brand_drop['category_name']=='箱包配饰']
    writer = pd.ExcelWriter("C:/Users/Administrator/Desktop/{}bade.xls".format(weekend))
    a.to_excel(excel_writer=writer, sheet_name='母婴世界')
    b.to_excel(excel_writer=writer, sheet_name='营养保健')
    c.to_excel(excel_writer=writer, sheet_name='个护美妆')
    d.to_excel(excel_writer=writer, sheet_name='环球美食')
    e.to_excel(excel_writer=writer, sheet_name='居家日用')
    # f.to_excel(excel_writer=writer, sheet_name='箱包配饰')
    writer.save()
    writer.close()


def parse_selfridges():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    self = db['self']
    weekend = datetime.date.today()
    self = pd.DataFrame(list(self.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    self = self.drop(columns=['_id'])
    self = self.drop_duplicates('pro_website', 'first')
    self = self.loc[self['discount'] <= 0.85]
    self = self.groupby(['brand_name']).apply(sort_data)
    self.to_excel("C:/Users/Administrator/Desktop/{}selfridges.xls".format(weekend))


def parse_farfetch():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    far = db['farfetch']
    weekend = datetime.date.today()
    far = pd.DataFrame(list(far.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    far = far.drop(columns=['_id'])
    far = far.loc[far['discount'].str.contains('OFF')]
    far['discount'] = far['discount'].replace("% OFF", "", regex=True)
    far['discount'] = far['discount'].astype('float64')
    far['discount'] = far['discount']/100
    far = far.groupby(['brand_name']).apply(sort_data)
    far = far.drop_duplicates('pro_website', 'first')
    far.to_excel("C:/Users/Administrator/Desktop/{}farfetch.xlsx".format(weekend))


def parse_yohobuy():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    yoho = db['yoho']
    weekend = datetime.date.today()
    yoho = pd.DataFrame(list(yoho.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    yoho = yoho.drop(columns=['_id'])
    yoho = yoho.replace(r"\\n", "", regex=True)
    yoho = yoho.replace("¥", "", regex=True)
    yoho['pro_website'] = 'https:'+yoho.pro_website
    yoho = yoho.drop_duplicates('pro_website', 'first')
    yoho = yoho.loc[yoho['discount'] != 1]
    yoho = yoho.groupby(['brand_name']).apply(sort_data)
    order = ['brand_name', 'pro_title', 'pro_website', 'pro_pic',
             'pro_price_new', 'pro_price_old', 'offers', 'discount', 'create_time']
    yoho = yoho[order]
    yoho.to_excel("C:/Users/Administrator/Desktop/{}yohobuy.xls".format(weekend))


def parse_unineed():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    unineed = db['unineed']
    weekend = datetime.date.today()
    unineed = pd.DataFrame(list(unineed.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    unineed = unineed.drop(columns=['_id'])
    unineed = unineed.groupby(['brand_name']).apply(sort_data)
    order = ['brand_name', 'pro_title', 'pro_website', 'pro_pic',
             'pro_price_new', 'pro_price_old', 'offers', 'discount', 'create_time']
    unineed = unineed[order]
    unineed = unineed.drop_duplicates('pro_website', 'first')
    unineed.to_excel("C:/Users/Administrator/Desktop/{}unineed.xls".format(weekend))


def parse_kaola():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    kaola = db['kaola']
    weekend = datetime.date.today()
    data = pd.DataFrame(list(kaola.find({'create_time': {'$regex': '^.*{}.*$'.format(weekend)}})))
    data = data.drop(columns=['_id'])
    data = data.loc[data['shop_name'] == '网易考拉自营']
    data = data.dropna(how='all', subset=['price_ref'])
    data['price'] = data['price'].astype('float64')
    data['price_ref'] = data['price_ref'].astype('float64')
    data['offers'] = data['price_ref'] - data['price']
    order = ['category', 'brand', 'title', 'activity', 'shop_name', 'goods_url', 'img_url', 'price', 'price_ref',
             'offers', 'comment_count', 'comment_grade', 'sunburn_count', 'create_time']
    data = data[order]
    data = data.drop_duplicates('goods_url', 'first')
    data = data.groupby(['category', 'brand']).apply(get_top50)
    # print(data)
    a = data.loc[((data['category'] == '彩妆') | (data['category'] == '香水/香氛') | (data['category'] == '护肤') | (
                data['category'] == '面膜') | (data['category'] == '防晒修复'))]
    b = data.loc[((data['category'] == '奶粉') | (data['category'] == '纸尿裤/拉拉裤') | (data['category'] == '营养辅食') | (
                data['category'] == '宝宝用品/玩具图书') | (data['category'] == '童装童鞋') | (data['category'] == '孕妈必备'))]
    c = data.loc[((data['category'] == '洗发护发') | (data['category'] == '身体护理') | (data['category'] == '口腔护理') | (
                data['category'] == '女性护理') | (data['category'] == '成人健康'))]
    d = data.loc[((data['category'] == '潮品上新') | (data['category'] == '时尚型男') | (data['category'] == '精致女神'))]
    e = data.loc[((data['category'] == '乳品/咖啡/麦片/冲饮') | (data['category'] == '休闲零食') | (data['category'] == '茶/酒/饮料'))]
    writer = pd.ExcelWriter("C:/Users/Administrator/Desktop/{}kaola_sales.xlsx".format(weekend))
    a.to_excel(excel_writer=writer, sheet_name='美容彩妆')
    b.to_excel(excel_writer=writer, sheet_name='母婴儿童')
    c.to_excel(excel_writer=writer, sheet_name='个人护理')
    d.to_excel(excel_writer=writer, sheet_name='服饰鞋靴')
    e.to_excel(excel_writer=writer, sheet_name='环球美食')
    writer.save()
    writer.close()


def parse_tm():
    client = pymongo.MongoClient('localhost', 27017)
    db = client['haitao']
    tianmao = db['tianmao']
    weekend = datetime.date.today()
    data = pd.DataFrame(list(tianmao.find({'shop_name': {'$regex': '^.*旗舰.*$'}, 'create_time': '{}'.format(weekend)})))
    data = data.drop(columns=['_id'])
    data['z_price'] = data['z_price'].astype('float64')
    data['z_price_ref'] = data['z_price_ref'].astype('float64')
    data['offers'] = data['z_price_ref'] - data['z_price']
    order = ['category', 'brand_name', 'title', 'activity', 'shop_name', 'pc_goods_url', 'phone_goods_url', 'img_url',
             'z_price', 'z_price_ref', 'offers', 'sale_count', 'comment_count', 'create_time']
    data = data[order]
    data = data.drop_duplicates('pc_goods_url', 'first')
    data = data.groupby(['category', 'brand_name']).apply(get_tmTop50)
    print(data)


if __name__ == '__main__':
    # parse_pharmacy()
    # parse_bade()
    # parse_selfridges()
    # parse_farfetch()
    # parse_yohobuy()
    # parse_unineed()
    parse_kaola()
    # parse_tm()