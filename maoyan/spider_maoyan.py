
#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author:Analyst1981
@contact: Analyst1981@mail.com
@file: spider_maoyan.py
@time: 21/4/3
"""
from typing import Text
import requests
import json
import time
import os 
import re
import datetime
import pandas as pd
from multiprocessing import Pool

# 获取页面内容
def get_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
        'Cookie': 'Hm_lvt_703e94591e87be68cc8da0da7cbd0be2=1617457510; _lx_utm=utm_source%3DBaidu%26utm_medium%3Dorganic; _lxsdk_cuid=17897faf805c8-0bfe6df63757c8-c3f3568-144000-17897faf806c6; _lxsdk=CD75C980948211EBAF4F03259243E2DEA89592DB594C4CC5BD6E371043642BB0; __mta=248222264.1617457511470.1617457524667.1617457541177.3; Hm_lpvt_703e94591e87be68cc8da0da7cbd0be2=1617460206; _lxsdk_s=178981daeaa-a88-f0b-4c9%7C%7C3',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
    }
    try:
        r = requests.get(url, headers=headers)
        if(r.status_code==200):
            print(r.content)
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
def parse_data(html):
    #jtopy=json.dumps(html)
    json_data = json.loads(html)['hcmts']
    comments = []
    # 解析数据并存入数组
    try:
        for item in json_data:
            comment = []
            comment.append(item['nickName']) # 昵称
            comment.append(item['cityName'] if 'cityName' in item else '') # 城市
            comment.append(item['content'].strip().replace('\n', '')) # 内容
            comment.append(item['score']) # 星级
            comment.append(item['startTime'])
            comment.append(item['time']) # 日期
            comment.append(item['approve']) # 赞数
            comment.append(item['reply']) # 回复数
            if 'gender' in item:
                comment.append(item['gender'])  # 性别
            comments.append(comment)
        return comments
    except Exception as e:
        print(e)

# 保存数据，写入 csv
def save_data(comments):
    #if not os.path.isfile('comments.csv'):
        
    filename = 'comments.csv'
    dataObject = pd.DataFrame(comments)
    dataObject.to_csv(filename, mode='a', index=False, sep=',', header=False, encoding='utf_8_sig')

# 爬虫主函数
def main():
    #def main(offset):
    # 当前时间id,end_time
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 电影上映时间
    end_time = '2021-02-12 08:00:00'
    print("[{d}]- 开始运行程序 -".format(d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))) 
    while start_time > end_time:
        url ='https://m.maoyan.com/mmdb/comments/movie/1048268.json?_v_=yes&offset=15&startTime=' \
            + start_time.replace('  ', '%20')
        try:
            html = get_page(url)
        except Exception as e:  # 如果有异常,暂停一会再爬1328712
            time.sleep(1)
            html = get_page(url)
        
        comments = parse_data(html)
        start_time = comments[9][4]  # 获取每页中最后一条评论时间
        print(start_time)
        # 最后一条评论时间减一秒，避免爬取重复数据
        start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d  %H:%M:%S') + datetime.timedelta(seconds=-1)
        start_time = datetime.datetime.strftime(start_time, '%Y-%m-%d  %H:%M:%S')
        print(start_time)
        save_data(comments)

if __name__ == '__main__':
    """
    url=http://m.maoyan.com/mmdb/comments/movie/movieid.json?_v_=yes&offset=15&startTime=xxx
    movieid：网站中每部影片的唯一 id
    startTime：当前页面中第一条评论的时间，每页共有 15 条评论
    """
    #print("请输入影片的id号和上映时间，格式：年（2020）-月(01)-日(01)")
    #moveid =input("id号：")
    #movetime =input("上映时间：")
    pool = Pool()#创建一个进程池
    pool.map(main,[i*10 for i in range(10)])  #map方法创建进程（不同参数的main），并放到进程池中
    #main()