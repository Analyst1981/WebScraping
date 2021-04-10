#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import time
import random
import requests
import traceback
from time import sleep
from fake_useragent import UserAgent
from lxml import etree

def  get_fund(url):
    headers = {"User-Agent":UserAgent(verify_ssl=False).random}
    response = requests.get(url, headers=headers, timeout=10)
    if(response.status_code==200):
        return response
    else:
        print("无信息获取")
        exit()

def parse_fund(r,fundcode):
    parse = etree.HTML(r.text)  # 解析网页
    items = parse.xpath('//*[@id="articlelistnew"]/div')[1:91]
    for item in items:
        item = {
            '阅读': ''.join(item.xpath('./span[1]/text()')).strip(),
            '评论': ''.join(item.xpath('./span[2]/text()')).strip(),
            '标题': ''.join(item.xpath('./span[3]/a/text()')).strip(),
            '作者': ''.join(item.xpath('./span[4]/a/font/text()')).strip(),
            '时间': ''.join(item.xpath('./span[5]/text()')).strip()
            }
        with open(f'./{fundcode}.csv', 'a', encoding='utf_8_sig', newline='') as fp:
            fieldnames = ['阅读', '评论', '标题', '作者', '时间']
            writer = csv.DictWriter(fp, fieldnames)
            writer.writerow(item)
# 主函数
def main(fundcode,page):
    #fundcode =     #可替换任意基金代码
    url = f'http://guba.eastmoney.com/list,of{fundcode}_{page}.html'
    html = get_fund(url)
    parse_fund(html,fundcode)


if __name__ == '__main__':
    #fundcode =input("请输入基金代码：")
    fundcode =161725
    rangepage = 6373
    #rangepage = input("请输入范围页面：")
    for page in range(1,rangepage):   #爬取多页
        main(fundcode,page)
        time.sleep(random.uniform(1, 2))   #随机出现1-2之间的数，包含小数
        print(f"第{page}页提取完成")