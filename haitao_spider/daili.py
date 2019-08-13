import redis
import random
import requests
from fake_useragent import UserAgent


def get_proxies():
    rediscli = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)
    data = rediscli.hgetall('useful_proxy')
    proxies = ["https://{}".format(proxy.decode()) for proxy in data]
    # return random.choice(proxies)
    ua = UserAgent()
    while True:
        headers = {'user-agent': ua.random}
        proxy_1 = random.choice(proxies)
        proxy_2 = {
            "https": proxy_1,
                   }
        res = requests.get("https://www.baidu.com", proxies=proxy_2, headers=headers)
        if res.status_code == 200:
            print(proxy_1)
            return proxy_1