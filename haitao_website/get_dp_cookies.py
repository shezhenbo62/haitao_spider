# coding=utf-8
from selenium import webdriver
import time
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import csv
import redis
import pyautogui

rediscli = redis.StrictRedis(host='192.168.2.218', port=6379, db=0)
proxy_list = ['61.174.158.179:45740', '27.29.161.111:48421', '218.73.133.136:51045', '36.22.196.128:28984',
              '123.55.90.47:44970', '36.57.76.217:50298', '180.121.73.233:27906', '124.135.123.143:32286',
              '171.80.187.211:55049', '123.54.235.230:40689']
print(proxy_list[0].split(':')[0])
print(proxy_list[0].split(':')[1])

for i in range(10):
    try:
        profile = webdriver.FirefoxProfile()
        profile.set_preference('network.proxy.type', 1)
        profile.set_preference('network.proxy.http', proxy_list[i].split(':')[0])
        profile.set_preference('network.proxy.http_port', int(proxy_list[i].split(':')[1]))  # int
        profile.set_preference('network.proxy.ssl', proxy_list[i].split(':')[0])
        profile.set_preference('network.proxy.ssl_port', int(proxy_list[i].split(':')[1]))
        profile.update_preferences()
        driver = webdriver.Firefox(firefox_profile=profile)
        driver.maximize_window()
        driver.get('https://www.dianping.com')
        time.sleep(2)
        driver.get('https://m.dianping.com')
        time.sleep(2)
        driver.find_elements_by_xpath("//div[@class='Fix page']/a[8]")[0].click()
        time.sleep(1)
        # for j in range(100, 1200, 400):
        #     js = "var q=document.documentElement.scrollTop={}".format(j)
        #     driver.execute_script(js)
        #     time.sleep(1)
        driver.find_elements_by_xpath("//div[@class='shop-list']/a[1]")[0].click()
        time.sleep(1)
        # for k in range(100, 1020, 400):
        #     js = "var q=document.documentElement.scrollTop={}".format(k)
        #     driver.execute_script(js)
        #     time.sleep(1)
        # driver.find_elements_by_xpath("//div[@class='item-list vc-flex']/div[4]")[0].click()
        sele_cookies = driver.get_cookies()
        print(sele_cookies)
        cookies = {cookie.get('name'): cookie.get('value') for cookie in sele_cookies}
        dp_cookie = ''
        for k, v in cookies.items():
            name = k + '=' + v + ';'
            dp_cookie += name
        print(dp_cookie)
        if dp_cookie:
            rediscli.lpush('dp_cookies', dp_cookie.strip(';'))
            # with open('C:/Users/Administrator/Desktop/dp_cookies.csv', mode='a', encoding='utf-8', newline='') as f:
            #     writer = csv.writer(f)
            #     writer.writerow([dp_cookie.strip(';')])
        time.sleep(1)
        driver.get('about:preferences#privacy')
        driver.find_elements_by_xpath("//*[@id='clearSiteDataButton']")[0].click()
        time.sleep(1)
    except Exception as e:
        print(e)
        driver.quit()
        continue
    pyautogui.click(1174, 686, button='left', duration=1)
    pyautogui.press('enter')

    driver.quit()



