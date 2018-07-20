# -*- coding: utf-8 -*-
# @Time    : 2018/6/23 17:20
# @Author  : ILuffZhe
# @Software: PyCharm

import sys
import os
import logging
import time

import requests
import re
import pymysql
import urllib
from lxml import etree

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
        'charset': 'utf8',
        # 'cursorclass': pymysql.cursors.DictCursor
    }


class Ntf_Peo_Leader_Spider2(object):

    def __init__(self):
        self.start_url = "https://www.baidu.com/s?ie=utf-8&f=3&rsv_bp=1&rsv_idx=1&tn=baidu&wd=%s&oq=%s&rqlang=cn&rsv_enter=0"
        self.query_conn = pymysql.connect(**db_config)
        self.update_conn = pymysql.connect(**db_config)
        self.header = {
            "User-Agent": get_user_agent(),
            "Connection": "keep-alive",
            "Host": "baike.baidu.com",
            "Upgrade-Insecure-Requests": '1',
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q = 0.8"
        }

    def get_search_terms(self):
        query_sql = "select per_name, remark, per_type, per_id from ntf_peo_leader limit 200;"
        with self.query_conn.cursor() as cursor:
            cursor.execute(query_sql)
            per_list = cursor.fetchall()
        return per_list

    def load_page(self):
        per_list = self.get_search_terms()
        for person in per_list:
            name = person[0]
            per_type = person[2]
            per_id = person[3]
            condition = '百度百科'
            if not person[1]:
                continue
            if '|' in person[1]:
                entitle_list = person[1].split('|')
                for entitle in entitle_list:
                    detail_url = self.switch(entitle, name, condition)
                    if detail_url:
                        per_dict = self.detail_page(detail_url, entitle)
                        if per_dict:
                            self.store_data(per_dict, per_type, per_id)
                            break
            else:
                detail_url = self.switch(person[1], name, condition)
                if detail_url:
                    per_dict = self.detail_page(detail_url, person[1])
                    if per_dict:
                        self.store_data(per_dict, per_type, per_id)

    def switch(self, entitle, name, condition):
        params = {"person": (entitle + name + condition).decode('utf-8')}
        params_encoder = urllib.urlencode(params)
        full_url = self.start_url % (params_encoder[7:], params_encoder[7:])
        response = requests.get(full_url, headers=self.header)
        time.sleep(1)
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
        time.sleep(1)
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
            logging.info('词条不存在'+name)

    def image_handler(self, image_url, image_path):
        image_local_path = "images\\%s" % image_path
        # image_local_path = "C:\\Users\\zhehu.abcft\\Desktop\\imagess\\%s" % image_path
        urllib.urlretrieve(image_url, image_local_path)
        # with open(image_local_path, 'rb') as fp:
        #     content = fp.read()
        #     if content:
        #         # Uploader().upload_file(oss_path=image_path, filename=image_local_path)
        #         os.remove(image_local_path)
        #     else:
        #         os.remove(image_local_path)

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
        except requests.exceptions.ConnectionError:
            logging.info('词条遗漏' + url)
            return None
        except requests.exceptions.TooManyRedirects:
            logging.info('词条遗漏' + url)
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
            logging.info('词条遗漏' + url)
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
                except IndexError:
                    per_dict['image'] = None
                    logging.info("图片链接有误" + image_url)

            return per_dict
        else:
            logging.info("词条为空" + url)
            return None

    def store_data(self, per_dict, per_type, per_id):
        update_sql = 'update ntf_peo_leader set per_ename=%s,per_alias=%s,country=%s,nation=%s,native_place=%s,birth_day=%s,occupation=%s,university=%s,main_achievement=%s,high_edu=%s, image=%s, source_url=%s, source_id="2" where per_type=%s and per_id=%s;'
        if per_dict['nation']:
            if u'\u65cf' in per_dict['nation']:
                pass
            else:
                per_dict['nation'] += u'\u65cf'
        params = (
        per_dict['per_ename'], per_dict['per_alias'], per_dict['country'], per_dict['nation'], per_dict['native_place'],
        per_dict['birth_day'], per_dict['occupation'], per_dict['university'], per_dict['main_achievement'],
        per_dict['high_edu'], per_dict['image'], per_dict['source_url'], per_type, per_id)
        with self.update_conn.cursor() as cursor:
            try:
                cursor.execute(update_sql, params)
                self.update_conn.commit()
                print ("--working--" + str(per_id) + ':' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
                logging.info("secceed update" + per_type + '--' + str(per_id))
            except pymysql.err.DataError:
                logging.info("数据插入有误" + per_type + '--' + str(per_id))
            except pymysql.err.InterfaceError:
                self.update_conn = pymysql.connect(**db_config)


def main():
    start = time.time()
    spider_ntf = Ntf_Peo_Leader_Spider2()
    spider_ntf.load_page()
    print("耗时"+str(time.time()-start))


if __name__ == "__main__":
    main()
