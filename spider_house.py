#!/usr/bin/env python
# -*- coding: utf-8 -*-


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
import csv
import random


#函数功能：获取加密后真正访问地址
def geturl(base_url):
    text = getHtml(base_url)
    soup = BeautifulSoup(text, 'lxml')
    tag = soup.find_all("script")[3]
    index = str(tag).find("rfss")
    var_t4 = base_url
    var_t3 = str(tag)[index : index + 28]
    trueUrl = var_t4 + "?" + var_t3
    return trueUrl

#函数功能：获取该区的房源总页数    
def get_pages(url):
    trueUrl = geturl(url)
    #print(trueUrl)
    text = getHtml(trueUrl)
    html = etree.HTML(text)
    page = html.xpath("//div[@class='page_box']//span/text()")
    pages = int(page[-1].replace('共','').replace('页',''))
    return pages
#函数功能：获取网页源码
def getHtml(url):

    USER_AGENTS = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",]
    user_agent = random.choice(USER_AGENTS)
    headers = {

        'User-Agent': user_agent,
        #'Cookies' :
        }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        # content属性返回的类型是bytes
        html = response.content.decode('utf-8', errors='ignore')
        # text属性类型为string
        text = response.text
        return text
    except Exception as e:
        print(str(e))
        print("爬取失败！") 
#函数功能：解析采集房源信息   
def get_infollist(text):
    if text is None:

        return
    global time0
    house_info_list = []
    html = etree.HTML(text)
    dls = html.xpath("//dl[@dataflag='bg'or @dataflag='qyk']")
    #print(len(dls))
    for dl in dls:
        try:
            introduce = dl.xpath("./dd[1]/h4/a/@title")
            info = dl.xpath("./dd[1]/p[@class='tel_shop']/text()")
            huxing = info[0].strip()
            area = info[1].strip()
            fx = info[3].strip()
            if len(info) > 5:
                time0 = info[4].strip()
            name = (dl.xpath("./dd[1]//p[@class='add_shop']/a/@title"))[0].strip()
            totle_price = (dl.xpath("./dd[@class='price_right']//span/b/text()"))[0].strip()
            location = (dl.xpath("./dd[1]/p[@class='add_shop']/span/text()"))[0].strip()
            height = dl.xpath("./dd[1]/p[@class='clearfix label']/span//text()")
            h1 = " "
            for str in height:
                h1 = h1 + "|" + str
            house_info = name + ',' + time0 + ',' + fx + ',' + huxing + ',' + area + ',' + totle_price + "万" + "," + location + ','  + h1.strip()
            print(house_info)
            house_info_list.append(house_info)
        except:
            print("有问题！")
            pass
    return house_info_list 

# 保存CSV文件
def save_csv(house_info_list, fname):
    if house_info_list is None:
        return
    print("开始写入文件......")
    f = open(fname, 'a+', encoding='GBK', newline="")
    csv_writer = csv.writer(f)
    #csv_writer.writerow(["小区名称","时间", "方向", "户型", "面积", "总价", "位置","特色"])
    for line in house_info_list:
        csv_writer.writerow(line.split(','))
    f.close()

#函数功能：地理编码

def dlbm(fname):

    AK = "自己的密钥"
    USER_AGENTS = [
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",]
    
    linelist = []
    loc_dict = {}
    with open(fname, "r", encoding="gbk") as f:
        reader = csv.reader(f)
        for line in reader:
            address = line[7]
            if address.find(line[0]):
                address = line[7].split('-')[0] + "-" + line[0]
            else:
                address += line[0]
            print(address)

            if loc_dict.get(line[0]):
                print('已存在！')
                location1 = loc_dict[line[0]]
                lng = float(location1.split(',')[0])
                lat = float(location1.split(',')[1])
                # list = gcj02towgs84(lng, lat)
                line.append(lng)
                line.append(lat)
                print(line)
                linelist.append(line)
            else:
                url = "https://restapi.amap.com/v3/geocode/geo?address={}&output=JSON&key=".format(address)+AK+"&city=西安市"
                #print(url)
                tag = random.randint(1, 5)
                time.sleep(tag)
                user_agent = random.choice(USER_AGENTS)
                headers = {
                'User-Agent': user_agent
                }
                html = requests.get(url, headers=headers)
                data = html.json()
                geocodes =  data.get('geocodes')
                status = data.get('status')
                if status == '1' and len(geocodes) > 0:
                    location = geocodes[0].get('location')
                    loc_dict[line[0]] = location
                    lng = float(location.split(',')[0])
                    lat = float(location.split(',')[1])
                    #list = gcj02towgs84(lng, lat)
                    line.append(lng)
                    line.append(lat)
                    linelist.append(line)
                    print(line)
                elif status == 0:
                    print(data.get('info'))
    f.close()

    fname1 = fname.split('.')[0] + str(1) + '.' +  fname.split('.')[1]
    with open(fname1, "w", encoding="gbk", newline='') as f:
        writer = csv.writer(f)
        for line in linelist:
            writer.writerow(line)
#函数功能：CSV文件添加列与合并
def csv_merge():
    print(os.getcwd()) #获取当前工作路径
    csv_list = glob.glob('*.csv')
    print(u'共发现%s个CSV文件'% len(csv_list))
    print(u'正在处理............')

    for i in csv_list:
        df = pd.read_csv(i, encoding = "gbk", header=0, error_bad_lines = False)
        # 插入列：行政区名称
        df.insert(1, '区(开发区)', i.split('-')[-1].split('.')[0])
        df.to_csv(i, mode = 'w', index =False, encoding = "gbk")

    for i in csv_list:
        print(i)
        fr = open(i, 'r', encoding ='gbk').read()
        with open(u'西安二手房信息表.csv', 'a') as f:
            f.write(fr)
    print(u'合并完毕！')
#函数功能：CSV文件去重

def quchong(file):
    df = pd.read_csv(file, encoding = "gbk", header=0, error_bad_lines = False)
    datalist = df.drop_duplicates()
    datalist.to_csv(u'西安二手房信息表.csv', mode = 'w', encoding = "gbk", index =False)


if __name__ == '__main__':
    main()