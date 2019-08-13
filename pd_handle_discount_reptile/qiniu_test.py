import time
from qiniu import Auth
from qiniu import BucketManager
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from qiniu import Auth, PersistentFop, build_op, op_save, urlsafe_base64_encode
import numpy as np

pd.set_option('display.width', None)
pd.set_option('max_colwidth', 1000)


class Update_pic():
    """
    传入一个pandas的DateFrame，将其'图片列'的外链转换为七牛云链接
    """
    def __init__(self, df):
        # 公钥和私钥
        self.access_key = 'ZkvKXbBzPzgO5cy0rV6GKiXR48vR0pI-_3RtyTXR'
        self.secret_key = 'R1dkQeGJpRjunQ5vtr-uI8a5IuSmfpvZ4Fhd5nAB'
        # 储存空间名
        self.bucket_name = 'worm'
        # 存储连接主域名
        self.domin = 'https://worm.d88.tech/'
        # 传入的df数据
        self.df = df
        # 过期时间
        self.days = '30'
        # 认证
        self.q = Auth(self.access_key, self.secret_key)
        self.bucket = BucketManager(self.q)

    def read_old_url(self, df):
        """读取图片外链并转换为列表"""
        old_url_lists = df['pro_pic'].tolist()
        return old_url_lists

    def fetch_url(self, url):
        """抓取外链的图片上传到指定七牛云空间,成功返回七牛云连接，错误放回None"""

        try:
            # 抓取图片并上传
            ret, info = self.bucket.fetch(bucket=self.bucket_name, url=url)
            # 获取图片的key
            key = ret['key']
            # 设置过期时间，days参数为字符串类型
            ret, info = self.bucket.delete_after_days(self.bucket_name, key, self.days)
            # 拼接url并放回
            new_url = self.domin + key
            return new_url
        except Exception as e:
            return np.nan

    def new_urls(self, url_lists):
        """创建线程池进行多线程抓取上传"""
        # start = time.time()
        with ThreadPoolExecutor(max_workers=10) as executor:
            future = executor.map(self.fetch_url, url_lists)
            # 将future生成器转换为列表
            lists = list(future)
        # 替换旧的图片列表
        self.df['pro_pic'] = lists
        # print(time.time() - start)

    def replace_url(self):
        """入口调度函数"""
        old_url_lists = self.read_old_url(self.df)
        self.new_urls(old_url_lists)
        # 删除pro_pic列中含有空值的行
        self.df = self.df.dropna(how='all', subset=['pro_pic'])
        # 返回图片连接替换后的的DateFrame
        return self.df


if __name__ == '__main__':
    df = pd.read_excel(r'C:\Users\Administrator\Desktop\2019-04-23_苏宁自营_电子_result.xls')
    a = Update_pic(df)
    b = a.replace_url()
    print(b)
