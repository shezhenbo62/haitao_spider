# -*- coding:utf-8 -*-
import redis
from sqlalchemy import create_engine
import pandas as pd
import time,datetime


def time_decorator(run):
    def func(*args, **kwargs):
        start_time = time.time()
        run(*args, **kwargs)
        end_time = time.time()
        print('读取mysql数据写入到redis耗时%s秒' % (end_time - start_time))

    return func


class RedisCache(object):
    def __init__(self):
        # 指定Mysql数据库信息
        self.MYSQL_URI = 'mysql+pymysql://root:root@192.168.2.199:3306/d88_2.0_test'
        self.conn = create_engine(self.MYSQL_URI)
        # 指定Redis数据库信息
        self.rediscli = redis.StrictRedis(host='127.0.0.1', port=6379, db=4)

    def searched_redis(self):
        length1 = self.rediscli.strlen('d88_user_behavior')
        length2 = self.rediscli.strlen('d88_discount')
        return length1, length2

    @time_decorator
    def write_to_redis(self):
        # d88_user_behavior表建立缓存
        datas = pd.read_sql_table('d88_user_behavior', self.conn).to_dict('records')
        datas = str(datas).replace('Timestamp(', '').replace(')', '').replace("'", '"')
        self.rediscli.set('d88_user_behavior', datas)
        expire_time = datetime.timedelta(seconds=10)
        self.rediscli.expire('d88_user_behavior', expire_time)
        # # 利用redis的管道快速写入数据，效率提升四倍左右
        # with self.rediscli.pipeline() as pipe:
        #     # 事务开始
        #     pipe.multi()
        #     for index, item in enumerate(datas):
        #         pipe.rpush('d88_user_behavior', item)
        #
        #         # 每达到1000条提交到redis
        #         if index % 1000 == 0:
        #             pipe.execute()
        #             pipe.multi()
        #     pipe.execute()

        # d88_discount表建立缓存
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql_cmd = '''SELECT id,create_time,update_time,expiration_time FROM d88_discount WHERE expiration_time>'{}' 
                    AND status=2 ORDER BY create_time DESC;
                    '''.format(now_time)
        discounts = pd.read_sql_query(sql_cmd, self.conn).to_dict('records')
        discounts = str(discounts).replace('Timestamp(', '').replace(')', '').replace("'", '"')
        self.rediscli.set('d88_discount', discounts)
        self.rediscli.expire('d88_discount', expire_time)
        # with self.rediscli.pipeline() as pipe2:
        #     # 事务开始
        #     pipe2.multi()
        #     for k, v in enumerate(discounts):
        #         pipe2.rpush('d88_discount', v)
        #
        #         # 每达到1000条提交到redis
        #         if k % 1000 == 0:
        #             pipe2.execute()
        #             pipe2.multi()
        #     pipe2.execute()

    def main(self):
        length1, length2 = self.searched_redis()
        if length1 == 0 and length2 == 0:
            print('redis缓存过期，重新建立缓存')
            self.write_to_redis()
        else:
            print('redis缓存未过期')


if __name__ == '__main__':
    ca = RedisCache()
    while True:
        ca.main()
        time.sleep(1)
