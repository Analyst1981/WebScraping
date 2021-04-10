#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author:Analyst1981
@contact: Analyst1981@mail.com
@file: spider_douban.py
@time: 21/4/3
"""
from typing import Text
from lxml import etree
from bs4 import BeautifulSoup
from pandas.core.frame import DataFrame
import requests
import json
import time
import os 
import re
import datetime
import pandas as pd
from multiprocessing import Pool

# 获取页面内容  for url in url_ list:
def get_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
        'Cookie': 'Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1617457510; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; _lxsdk_cuid=17897faf805c8-0bfe6df63757c8-c3f3568-144000-17897faf806c6; _lxsdk=CD75C980948211EBAF4F03259243E2DEA89592DB594C4CC5BD6E371043642BB0; __mta=248222264.1617457511470.1617457524667.1617457541177.3; Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1617460206; _lxsdk_s=178981daeaa-a88-f0b-4c9%7C%7C3',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
    }
    try:
        r = requests.get(url, headers=headers)
        if(r.status_code==200):
            #print(r.content)
            return r.content
        else:
            print("无信息获取")
        
    except requests.HTTPError as e:
        print(e)
    except requests.RequestException as e:
        print(e)
    except:
        print("出错了")

# 解析接口返回数据
def parse_data(data):
    filename = 'comments.csv'
    df = pd.DataFrame(columns=['时间','评分','标题','内容'])  
    soup = BeautifulSoup(data,'html.parser')
    review_list = soup.find_all(class_="main review-item")
    for i in range(len(review_list)):
        rank = review_list[i].select('span')[0].get('title')
        time =review_list[i] .select('span')[1].get('content')
        title =review_list[i].select('h2>a')[0].text
        a = review_list[i].find_all(
            class_='short-content')[0].contents[-3].replace("\n", "" )
        content= "".join(a.split( ))
        df = df.append({'时间': time,
                        '评分': rank,
                        '标题': title,
                        '内容': content},ignore_index=True)
    df.to_csv(filename, mode='a', index=True, sep=',',encoding='utf_8_sig')



# 爬虫主函数
def main(page):
    #def main(offset):

    print("[{d}]- 开始运行程序 -".format(d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))) 
    
    url ='https://movie.douban.com/subject/30458949/reviews?start='+ str(20*page)
    try:
        html = get_page(url)
    except Exception as e:  # 如果有异常,暂停一会再爬1328712
        time.sleep(1)
        html = get_page(url)
        
    parse_data(html)
        
    #save_data(comments)

if __name__ == '__main__':
   
    #print("请输入影片的id号和上映时间，格式：年（2020）-月(01)-日(01)")
    #moveid =input("id号：")
    #movetime =input("上映时间：")
    #pool = Pool()#创建一个进程池
    #pool.map(main,[i*10 for i in range(10)])  #map方法创建进程（不同参数的main），并放到进程池中
    page=0
    main(page)