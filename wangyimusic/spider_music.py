#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scrapy
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
import time
import re

class WyyspiderSpider(scrapy.Spider):
    name = 'wyySpider'
    allowed_domains = ['163.com']
    start_urls = ['https://music.163.com/playlist?id=19xxxxx7']

    def getCookie(self):
        # 获取谷歌的驱动,参数为刚刚驱动程序的位置
        driver = webdriver.Chrome("C:/ProgramData/Anaconda3/Lib/site-packages/selenium/webdriver/chrome/chromedriver.exe")
        # -----------------selenium自动登录-----------------------

        # 打开谷歌然后访问指定的网站
        driver.get("https://music.163.com/")

        # 最大化,睡眠是怕网速慢没加载出来
        driver.maximize_window()
        time.sleep(1)
  # 以下坐标以自己的电脑为准
        # 鼠标从(0,0)向x(1435px),y(35px)移动,用左键点击一下
        ActionChains(driver).move_by_offset(1435, 35).click().perform()
        time.sleep(0.3)

        # 点击其他方式
        ActionChains(driver).move_by_offset(-480, 575).click().perform()
        time.sleep(0.3)

        # 同意条款
        ActionChains(driver).move_by_offset(-218, -10).click().perform()
        time.sleep(0.3)

        # 手机登录
        ActionChains(driver).move_by_offset(107, -100).click().perform()
        time.sleep(0.3)

        # 输入账号密码
        # 通过css选择器获取id为"p"的标签,然后send_keys就是模拟输入一些信息
        driver.find_element_by_css_selector("#p").send_keys("账号")
        driver.find_element_by_css_selector("#pw").send_keys("密码")
        time.sleep(0.3)

        # 点击登录
        ActionChains(driver).move_by_offset(110, 15).click().perform()
        time.sleep(1)

        # 找到头像悬浮
        img = driver.find_element_by_css_selector("div.head:nth-child(1) > img:nth-child(1)")
        ActionChains(driver).move_to_element(img).perform()
        time.sleep(0.5)
        # 点击我的主页
        ActionChains(driver).move_by_offset(0, 40).click().perform()
        time.sleep(0.5)
        # # 点击喜欢的音乐
        # ActionChains(driver).move_by_offset(-870, 830).click().perform()
        # time.sleep(0.3)


        # -----------------selenium自动登录-----------------------

        # 将driver获取的字典类型的cookie提取name和value封装成字符串
        # 临时存放每个拼接好的key=value字符串
        temp = []

        # 遍历driver给的cookies字典
        for i in driver.get_cookies():
            temp.append(i['name'] + "=" + i['value'])

        # 返回字符串cookie
        return ';'.join(temp)

    def start_requests(self):
        # 定义请求头的时候调用一下getCookie获取一下cookie
        headers = {
            'Cookie': self.getCookie(),
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.104 Safari/537.36'
        }
        # 注意url是个列表这里拿下标[0],然后把headers请求头塞进去,交给parse函数
        yield scrapy.Request(url=self.start_urls[0], headers=headers, callback=self.parse)

    def parse(self, response):
        # 匹配歌曲名的正则表达式
        patt = re.compile(r'<a href="/song.id=.*?">([^<|{]*?)</a>')

        # 找到所有歌曲名
        listdata = re.findall(patt, response.text)

        # 把数据写进txt文件
        with open(file="response.txt", mode="w+", encoding="utf-8") as file:
            for item in listdata:
                file.write(item+"\n")

if __name__ == '__main__':
    join=WyyspiderSpider()
    join.start_requests