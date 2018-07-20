# -*- coding: utf-8 -*-
# @Time    : 2018/6/23 17:20
# @Author  : ILuffZhe
# @Software: PyCharm

import sys
import os
import logging
import time

import requests
import threading
import Queue
import re
import pymysql
import urllib
from lxml import etree
from DBUtils.PooledDB import PooledDB  # 数据库连接池

from headers import get_user_agent
import config
# from uploader import Uploader

reload(sys)
sys.setdefaultencoding('utf-8')
logging.basicConfig(level=logging.INFO, filename="ntf_peo_leader_spider2.log", filemode='w',
                    format='%(asctime)s-%(name)s-%(levelname)s [line:%(lineno)d]-%(message)s')
# db_config = config.dc
db_config = {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'ILuff',
        'password': '950522',
        'db': 'manager_peo',
        'charset': 'utf8'
    }


class Ntf_Peo_Leader_Spider2(threading.Thread):

    def __init__(self, info_queue, thread_name):
        super(Ntf_Peo_Leader_Spider2, self).__init__()
        self.start_url = "https://www.baidu.com/s?ie=utf-8&f=3&rsv_bp=1&rsv_idx=1&tn=baidu&wd=%s&oq=%s&rqlang=cn&rsv_enter=0"
        self.mysql_pool = PooledDB(pymysql, 5, **db_config)
        self.header = {
            "User-Agent": get_user_agent(),
            "Connection": "keep-alive",
            "Host": "baike.baidu.com",
            "Upgrade-Insecure-Requests": '1',
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q = 0.8"
        }
        self.info_queue = info_queue
        self.thread_name = thread_name

    def run(self):
        print("--爬虫线程"+self.thread_name+"启动--")
        while True:
            try:
                # 当队列为空时，抛出Queue.Empty的异常
                info = self.info_queue.get(False)
                per_info = list(info)
                self.load_page(per_info)
            except Queue.Empty:
                break
        print("--爬虫线程" + self.thread_name + "结束--")

    def load_page(self, per_info):
        name = per_info[0]
        per_type = per_info[2]
        per_id = per_info[3]
        condition = '百度百科'
        if not per_info[1]:
            return
        if '|' in per_info[1]:
            entitle_list = per_info[1].split('|')
            for entitle in entitle_list:
                detail_url = self.switch(entitle, name, condition)
                if detail_url:
                    per_dict = self.detail_page(detail_url, entitle)
                    if per_dict:
                        self.store_data(per_dict, per_type, per_id)
                        break
        else:
            detail_url = self.switch(per_info[1], name, condition)
            if detail_url:
                per_dict = self.detail_page(detail_url, per_info[1])
                if per_dict:
                    self.store_data(per_dict, per_type, per_id)

    def switch(self, entitle, name, condition):
        params = {"person": (entitle + name + condition).decode('utf-8')}
        params_encoder = urllib.urlencode(params)
        full_url = self.start_url % (params_encoder[7:], params_encoder[7:])
        response = requests.get(full_url, headers=self.header)
        time.sleep(0.2)
        content = response.text
        html = etree.HTML(content)
        node_list = html.xpath('//div[contains(@class, "c-container")]')
        detail_url = None
        for node in node_list:
            title_list = node.xpath('./h3/a')
            if title_list:
                title = title_list[0].xpath(('string(.)'))
                if (title.startswith(name+'(') and title.endswith(condition)) or (title == name+'_'+condition):
                    detail_url = node.xpath('./h3/a/@href')[0]
                    break
        if detail_url:
            return detail_url
        else:
            detail_url = self.second_switch(name, condition)
            return detail_url

    def second_switch(self, name, condition):
        params = {"person": (name + condition).decode('utf-8')}
        params_encoder = urllib.urlencode(params)
        full_url = self.start_url % (params_encoder[7:], params_encoder[7:])
        response = requests.get(full_url, headers=self.header)
        time.sleep(0.2)
        content = response.text
        html = etree.HTML(content)
        node_list = html.xpath('//div[contains(@class, "c-container")]')
        detail_url = None
        for node in node_list:
            title_list = node.xpath('./h3/a')
            if title_list:
                title = title_list[0].xpath(('string(.)'))
                if (title.startswith(name+'(') and title.endswith(condition)) or (title == name+'_'+condition):
                    detail_url = node.xpath('./h3/a/@href')[0]
                    break
        if detail_url:
            return detail_url
        else:
            logging.info('{0}--{1}词条不存在'.format(name, condition))

    def image_handler(self, image_url, image_path):
        # image_local_path = "images\\%s" % image_path
        image_local_path = "C:\\Users\\zhehu.abcft\\Desktop\\imagess\\%s" % image_path
        try:
            urllib.urlretrieve(image_url, image_local_path)
        except IOError as e:
            logging.error("{0}-导致图片丢失-{1}".format(e, image_url))
        # with open(image_local_path, 'rb') as fp:
        #     content = fp.read()
        #     if content:
                # Uploader().upload_file(oss_path=image_path, filename=image_local_path)
                # os.remove(image_local_path)
            # else:
                # os.remove(image_local_path)

    def detail_page(self, url, entitle):
        per_dict = {
            'per_ename': None,
            'per_alias': None,
            'country': None,
            'nation': None,
            'native_place': None,
            'birth_day': None,
            'occupation': None,
            'university': None,
            'main_achievement': None,
            'high_edu': None,
            'image': None,
            'source_url': None
        }
        dict_web = {}
        try:
            response = requests.get(url, headers=self.header)
        except requests.exceptions.ConnectionError as e:
            logging.error('{0}导致词条{1}遗漏'.format(e, url))
            return None
        except requests.exceptions.TooManyRedirects as e:
            logging.error('{0}导致词条{1}遗漏'.format(e, url))
            return None
        content = response.content
        try:
            html = etree.HTML(content)
        except TypeError or AttributeError:
            return None
        entitle_list = html.xpath('//div[@class="main-content"]')
        if entitle_list:
            entitle_ = entitle_list[0].xpath('string(.)')
            if entitle in entitle_:
                pass
            else:
                return None
        else:
            return None
        if 'aladdin' in response.url:
            per_dict['source_url'] = response.url[:-11]
        else:
            per_dict['source_url'] = response.url
        key_list = html.xpath('//div[@class="basic-info cmn-clearfix"]//dt/text()')
        value_list = html.xpath('//div[@class="basic-info cmn-clearfix"]//dd')
        if key_list and value_list:
            for key, value in zip(key_list, value_list):
                true_value = value.xpath('string(.)').strip()
                dict_web[key.replace('    ', '')] = re.sub(r'\[\d+\]', '', true_value)
            for w_key in [u'\u5916\u6587\u540d', u'\u82f1\u6587\u540d\uff1a']:
                if w_key in dict_web.keys():
                    per_dict['per_ename'] = dict_web[w_key]
                    break
            for w_key in [u'\u522b\u540d', u'\u522b\u540d\uff1a', u'\u82b1\u540d', u'\u6635\u79f0', u'\u7b14\u540d',
                          u'\u7f51\u540d']:
                if w_key in dict_web.keys():
                    per_dict['per_alias'] = dict_web[w_key]
                    break
            for w_key in [u'\u56fd\u7c4d', u'\u56fd\u7c4d\uff1a', u'\u56fd\u5bb6', u'\u4e2d\u56fd']:
                if w_key in dict_web.keys():
                    per_dict['country'] = dict_web[w_key]
                    break
            for w_key in [u'\u6c11\u65cf', u'\u6c11\u65cf\uff1a']:
                if w_key in dict_web.keys():
                    per_dict['nation'] = dict_web[w_key]
                    break
            for w_key in [u'\u51fa\u751f\u5730', u'\u51fa\u751f\u5730\uff1a']:
                if w_key in dict_web.keys():
                    per_dict['native_place'] = dict_web[w_key]
                    break
            for w_key in [u'\u51fa\u751f\u65e5\u671f', u'\u51fa\u751f\u5e74\u6708', u'\u51fa\u751f\u5e74\u6708\uff1a',
                          u'\u51fa\u751f\u5e74\u4efd', u'\u51fa\u751f\u65f6\u95f4']:
                if w_key in dict_web.keys():
                    per_dict['birth_day'] = dict_web[w_key]
                    break
            for w_key in [u'\u804c\u4e1a', u'\u804c\u4e1a\uff1a', u'\u804c\u4e1a\u0031']:
                if w_key in dict_web.keys():
                    per_dict['occupation'] = dict_web[w_key]
                    break
            for w_key in [u'\u6bd5\u4e1a\u9662\u6821', u'\u6bd5\u4e1a\u9662\u6821\uff1a', u'\u6bd5\u4e1a\u5927\u5b66',
                          u'\u6bd5\u4e1a\u673a\u6784', u'\u5927\u5b66']:
                if w_key in dict_web.keys():
                    per_dict['university'] = dict_web[w_key]
                    break
            for w_key in [u'\u4e3b\u8981\u6210\u5c31', u'\u6210\u5c31']:
                if w_key in dict_web.keys():
                    per_dict['main_achievement'] = dict_web[w_key]
                    break

            per_dict['high_edu'] = dict_web[
                u'\u6700\u9ad8\u5b66\u5386'] if u'\u6700\u9ad8\u5b66\u5386' in dict_web.keys() else None
            image_url = html.xpath('//div[@class="side-content"]/div[1]/a/img/@src')
            if image_url:
                try:
                    image_path = image_url[0][-40:]
                    per_dict['image'] = image_path
                    self.image_handler(image_url[0], image_path)
                except IndexError as e:
                    per_dict['image'] = None
                    logging.error("{0}--图片链接格式有误--{1}".format(e, image_url))
            return per_dict
        else:
            return None

    def store_data(self, per_dict, per_type, per_id):
        update_sql = 'update ntf_peo_leader set per_ename=%s,per_alias=%s,country=%s,nation=%s,native_place=%s,birth_day=%s,occupation=%s,university=%s,main_achievement=%s,high_edu=%s, image=%s, source_url=%s, source_id="2" where per_type=%s and per_id=%s;'
        if per_dict['nation']:
            if u'\u65cf' in per_dict['nation']:
                pass
            else:
                per_dict['nation'] += u'\u65cf'
        params = (per_dict['per_ename'], per_dict['per_alias'], per_dict['country'], per_dict['nation'], per_dict['native_place'],
                  per_dict['birth_day'], per_dict['occupation'], per_dict['university'], per_dict['main_achievement'],
                  per_dict['high_edu'], per_dict['image'], per_dict['source_url'], per_type, per_id)
        # 尽量不使用with方法：不然会抛出Cursor Closed的错误
        conn = self.mysql_pool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(update_sql, params)
            conn.commit()
            print("成功更新一条数据:{0}--{1}--{2}".format(str(per_id), per_type, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
        except pymysql.err.DataError as e:
            logging.error("{0}:{1}--{2}".format(e, per_type, str(per_id)))
        except pymysql.err.InterfaceError as e:
            logging.error("{0}:{1}--{2}".format(e, per_type, str(per_id)))
        cursor.close()
        conn.close()


def get_search_terms():
    query_sql = "select per_name, remark, per_type, per_id from ntf_peo_leader limit 300;"
    conn = pymysql.connect(**db_config)
    with conn.cursor() as cursor:
        cursor.execute(query_sql)
        per_list = cursor.fetchall()
    cursor.close()
    return per_list


def main():
    start = time.time()
    per_list = get_search_terms()
    # 将所有的任务信息保存至队列中
    info_queue = Queue.Queue()
    for peo_info in per_list:
        info_queue.put(peo_info)
    # 创建5个爬虫线程
    crawl_thread = []
    thread_name = ['No.1', 'No.2', 'No.3', 'No.4', 'No.5']
    length = len(thread_name)
    for i in range(0, length):
        t1 = Ntf_Peo_Leader_Spider2(info_queue, thread_name[i])
        crawl_thread.append(t1)
    for i in range(0, length):
        crawl_thread[i].start()
    # 线程进入阻塞等待
    for i in range(0, length):
        crawl_thread[i].join()
    print("--所有爬虫线程结束--共耗时{0}".format(str(time.time()-start)))


if __name__ == "__main__":
    main()