# -*- coding: utf-8 -*-

# Scrapy settings for haitao_spider project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'haitao_spider'

SPIDER_MODULES = ['haitao_spider.spiders']
NEWSPIDER_MODULE = 'haitao_spider.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

LOG_LEVEL = 'DEBUG'
# LOG_FILE = "./log.log"

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
# 'Connection': 'keep-alive',
# 'Cache-Control': 'max-age=0',
# 'Upgrade-Insecure-Requests': '1',
# 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
# 'Referer': 'http://www.selfridges.com/CN/zh/',
# 'Accept-Encoding': 'gzip, deflate',
# 'Accept-Language': 'zh-CN,zh;q=0.9',
# }

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    'haitao_spider.middlewares.HaitaoSpiderSpiderMiddleware': 543,
#}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   # 'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 200,
    'haitao_spider.middlewares.RandomUserAgentMiddlewares':100,
    # 'haitao_spider.middlewares.ABProxyMiddleware': 1,
    # 'haitao_spider.middlewares.LogPrint': 200
}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
   'haitao_spider.pipelines.HaitaoSpiderPipeline': 300,
    # 'scrapy_redis.pipelines.RedisPipeline': 400,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 1
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
# HTTPERROR_ALLOWED_CODES = [301]

MONGODB_SERVER = "192.168.2.220"
MONGODB_PORT = 27017
MONGODB_DB = "haitao"
# MONGODB_COLLECTION = "bade"
MONGODB_COLLECTION = "yoho"
# MONGODB_COLLECTION = "self"
# MONGODB_COLLECTION = "unineed"

# # 去重类的引入
# DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
# # redis调度器的引用
# SCHEDULER = "scrapy_redis.scheduler.Scheduler"
# # redis是否本地化的开关
# SCHEDULER_PERSIST = True
#
# # 分布式爬虫需要配置要访问的redis的主机
# REDIS_URL = "redis://192.168.2.220:6379"
