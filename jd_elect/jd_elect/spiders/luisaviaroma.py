# -*- coding: utf-8 -*-
import scrapy
from jd_elect.items import JdElectItem
from urllib import parse
import re
import datetime
from urllib import parse
from fake_useragent import UserAgent


class KaolaSpider(scrapy.Spider):
    name = 'luisa'
    allowed_domains = ['www.luisaviaroma.com']
    start_urls = ['https://www.luisaviaroma.com/zh-cn/shop/%E7%94%B7%E5%A3%AB?lvrid=_gm_s',
                  'https://www.luisaviaroma.com/zh-cn/shop/%E5%A5%B3%E5%A3%AB?lvrid=_gw_s',
                  'https://www.luisaviaroma.com/zh-cn/shop/%E7%94%B7%E5%AD%A9?lvrid=_gkb_s',
                  'https://www.luisaviaroma.com/zh-cn/shop/%E5%A5%B3%E5%AD%A9?lvrid=_gkg_s',
                  'https://www.luisaviaroma.com/zh-cn/shop/%E5%AE%B6%E5%B1%85?lvrid=_ge_s',
                  ]
    ua = UserAgent()

    def start_requests(self):
        cookies = {
            '_abck': '96F8B431D035C4085CBA1667239D1BCB~0~YAAQEGgDF1ua9LdpAQAAc3MfuAFgyO/V6SBwZNUqo9qxZO9znT+frFDs0Jm1n0lTTUGJ6r4YC5ybuufvWT46iySq1M3Symmf/1d2St0EdVSgLMDtJXiNygnXCZECOodhvxiL8+lp4EeNzNyW6XCAzxbi28tYgtl3pZHvM9gzVvTE2Sexsw6CfLQsE5AEAq41+bsTKN/vH5Lb80tWknP+2/I42WFdm/B2J2CDA66f9NwJAvXdDdS5DCzmHl2B14MqDqDKTZTcfZFH5TVvkPUUJ2C4RzKbKGaUqfNAGfHyRbx28dN0UbYzh4mQHVMbUsw=~-1~-1~-1',
            'LVR_UserData': 'Ver=6&cty=CN&curr=EUR&vcurr=CNY&lang=ZH',
            'LVR_User': 'ver=1',
            'LVR_Ref': 'faurl=aHR0cDovL3d3dy5sdWlzYXZpYXJvbWEuY29tL1VzZXJTZXNzaW9uLmFzcHg/dj0yMDE5MDMxODE0NTAyOA==&laurl=aHR0cDovL3d3dy5sdWlzYXZpYXJvbWEuY29tL1VzZXJTZXNzaW9uLmFzcHg/dj0yMDE5MDMyOTE0MzI0Ng==&fadate=2019-03-26 04:50:49&ladate=2019-04-02 12:13:17&faip=106.120.24.36&laip=183.12.103.133',
            'LVR_MarketingCookies': 'true',
            'LVR_AnalyticsCookies': 'true',
            '_dy_ses_load_seq': '26324%3A1554200042542',
            '_dy_c_exps': '',
            '_dy_soct': '351053.575000.1554200038*246797.371038.1554200042*246798.371039.1554200042*246799.371040.1554200042*246800.371041.1554200042*357258.590110.1554200042*135768.190303.1554200042*319900.614228.1554200043',
            'tc_scoring_pv': '10',
            'LVR_BC': 'viewed',
            'TCID': '201932115143681236062',
            '_dyid': '1553742199413907881',
            '_dycst': 'dk.w.f.ws.',
            '_dy_geo': 'CN.AS.CN_GD.CN_GD_Shenzhen',
            '_dy_df_geo': 'China..Shenzhen',
            '_dy_toffset': '0',
            '_dycnst': 'dg',
            '_dy_c_att_exps': '',
            'LVR_FT': 'viewed',
            'stc114797': 'env:1554200043%7C20190503101403%7C20190402104403%7C1%7C1043009:20200401101403|uid:1553572284676.1658491204.7789621.114797.738374274.:20200401101403|srchist:1043009%3A1554200043%3A20190503101403:20200401101403|tsa:1554200043839.646325403.9949706.36852227699567985.:20190402104403',
            '_gcl_au': '1.1.306270838.1553572291',
            '_ga': 'GA1.2.1857062446.1553572291',
            'LVR_Touchpoint': 'url=/zh-cn/shop/%e5%a5%b3%e5%a3%ab$query=lvrid=_gw_s$referrer=https://www.luisaviaroma.com/zh-cn/?aka_re=1',
            'bm_sz': '9FED7F5E18B35EC9F634469252F457F9~YAAQFgEPF8vod9lpAQAAkYaK3QPhfTqiy1huVJ+ojODrlWV4KaKulfzAwYrirDuoxoBZQFcA4vLJ2PxFyk4zoh6xTASzOojyOhk9hQ14l70gdGPIvqFJmvgThqXMTUR4s4pumox3iciDeckyoFHFJZBPogVyQdCKeywBPM5AYJ9D40QUwQhvZV71u/rySdVgmfFN3DY=',
            'ak_bmsc': 'E3D04906B39165FBEAED7A6C5A4C9D32170F011692610000E535A35C13E5121A~plFf2PaZCqvwM0jmx+GNB8zuhjIbd8NWOQIY7PwaU0LfYR+xm2xBJD1k+7djjNE625QFKrdVZZnOcO9mnBx2ILESooE965fdbGqCSmSH3ZPcs+XeNmA7AcP6KFg9sRhaOglA9pCpdJrW/oNj86T41uHwr9olFRzcLY06tqnjvTRGbr5sBNF1jqXpa0jKwwVQtc3sZFZH8fqsEAh1jj7ZfirtpADRu6pCwqJO6gnOBftuqa1a39iKB3nwE3RANeY3Iu',
            'bm_mi': 'BEB563289B7D9091D2DB09816D462D9C~h9HzhaEJ15FNoTey0CrT8A64REgHR6ZFZ44Jtr3x3Fznbpnhnam6XXhjm6yqIzzRGErJR+550fskgFbO12EPePtJvNvn+DKThzFr4vQzYCzkiMQYUNPncJzLb+U3iIGzHxZbDKHDZTWC675/HwD9x/sF6xiWohgAi0XXbEuRixcqvLzoaQ7+lixCErZatuU8weWFjx4YvairJxu2dav/tRwY56HDVSRze2cyCh5/+oSv5nrYcKkLe6IXzOY42NOo0s1gsXi+WJc5wfvtTmnBeQhySOtjcCTDQ1nKfpp1zEs=',
            'bm_sv': 'F1B9CF807996FFB008588CBBBCA915F4~GpShQKq4/pZaGPu3kuMDGKh2P7m2xY6fEI5aKa47ywro7uViAv2YAYfPPW+0pJczuQH5edg/Y5rLJU/r5sMR/x78zRn0Jt/msOFIhvyFa+74Uh4dszBuKIFzaRsbW+zW3zzlNAgSATYcVF8/Q1qaSFh0XSFULMPo2V6uPB+UxHc=',
            'ASP.NET_SessionId': 'vvpsohx342yvswuhgigcblee',
            '_dy_csc_ses': 't',
            'TCSESSION': '201942181414754975677',
            'RT': 'sl=3&ss=1554200038408&tt=0&obo=3&bcn=%2F%2F173e2513.akstat.io%2F&sh=1554200043805%3D3%3A3%3A0%2C1554200043766%3D2%3A2%3A0%2C1554200041760%3D1%3A1%3A0&dm=www.luisaviaroma.com&si=cd7fbe56-4520-4675-8aa2-141177d19ef7&ld=1554200043806',
            '_dyjsession': '3975e5c52924d85b5f2433ecd70499a3',
        }

        for url in self.start_urls:
            yield scrapy.Request(url,
                                 callback=self.parse,
                                 cookies=cookies)

    def parse(self, response):
        item = JdElectItem()
        a_list = response.xpath("//div[@id='div_lp_body']//a")
        for a in a_list:
            item['brand_name'] = a.xpath("./span/span[@itemprop='brand']/span/text()").extract_first()
            item['title'] = a.xpath("./span/span[@itemprop='name']/text()").extract_first()
            item['goods_url'] = a.xpath("./@href").extract_first()
            if item['goods_url']:
                item['goods_url'] = 'https://www.luisaviaroma.com' + item['goods_url']
            item['img_url'] = a.xpath("./span/span[@class='picture']/span/img[1]/@data-src").extract_first()
            if item['img_url']:
                item['img_url'] = 'https:' + item['img_url']
            else:
                item['img_url'] = a.xpath("./span/span[@class='picture']/span/img[1]/@src").extract_first()
                item['img_url'] = parse.urljoin(response.url, item['img_url'])
            item['price'] = a.xpath("./span/span[@class='catalog__item__price price']/span/span[last()]/span/text()").extract_first()
            if item['price']:
                item['price'] = re.findall(r'(\d+)', item['price'])[0]
            item['discount'] = a.xpath("./span/span[@class='catalog__item__price price']/span/span[last()-1]/text()").extract_first()
            if item['discount']:
                item['discount'] = re.findall(r'(\d+%)', item['discount'])[0]
            item['price_ref'] = a.xpath("./span/span[@class='catalog__item__price price']/span/span[1]/text()").extract_first()
            # if item['price_ref']:
            #     item['price_ref'] = re.findall(r'(\d+)', item['price_ref'])[0]
            item['create_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            yield item
