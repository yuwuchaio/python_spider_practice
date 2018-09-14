import re
import urllib.request
import random
import threading
from bs4 import BeautifulSoup
import sqlite3
import ast
from pypinyin import lazy_pinyin

baseUrl = 'https://sh.lianjia.com/ershoufang/'
newest = 'co32'
price = 'co21'
unit_price = 'co41'
area = 'co11'
most_welcome = 'co52'

#headers
hds=[{'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.1 31 Version/11.11'}]
# 上海各区
regions = [u'浦东', u'闵行', u'宝山', u'徐汇', u'普陀', u'杨浦', u'长宁', u'松江', u'嘉定', u'黄浦', u'静安', u'闸北', u'虹口', u'青浦', u'奉贤', u'金山',u'崇明', u'上海周边']

lock = threading.Lock()

class SQLiteWraper(object):
    """
       数据库的一个小封装，更好的处理多线程写入
       """
    def __init__(self,path,command='',*args,**kwargs):
        self.lock = threading.RLock()
        self.path = path

        if command != '':
            conn = self.get_conn()
            cu = conn.cursor()
            cu.execute(command)

    def get_conn(self):
        conn = sqlite3.connect(self.path)
        conn.text_factory=str
        return conn

    def conn_close(self,conn=None):
            conn.close()

    def conn_trans(func):
        def connection(self, *args, **kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self, *args, **kwargs)
            self.conn_close(conn)
            self.lock.release()
            return rs
        return connection

    @conn_trans
    def execute(self, command, method_flag=0, conn=None):
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0], command[1])
            conn.commit()
        except sqlite3.IntegrityError as e:
            # print e
            return -1
        except Exception as e:
            print(e)
            return -2
        return 0

    @conn_trans
    def fetchall(self, command="select name from xiaoqu", conn=None):
        cu = conn.cursor()
        lists = []
        try:
            cu.execute(command)
            lists = cu.fetchall()
        except Exception as e:
            print(e)
            pass
        return lists

def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list= [u'小区名称', u'大区域', u'小区域', u'建造时间']
    t=[]

    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?)",t)
    return command

def gen_chengjiao_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'链接',u'小区名称',u'户型',u'面积',u'朝向',u'装修',u'楼层',u'建造时间',u'签约时间',u'签约单价',u'签约总价']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    command= (r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?)", t)
    return command


def xiaoqu_spider(db_xq, url_page='https://sh.lianjia.com/xiaoqu/yangpu/'):
    """
      爬取页面链接中的小区信息
      """
    print('开始爬小区信息%s' % url_page)
    try:
        request = urllib.request.Request(url_page, headers=hds[random.randint(0, len(hds)-1)])
        source_code = urllib.request.urlopen(request, timeout=10).read()
        soup = BeautifulSoup(source_code, features='html.parser')
    except urllib.request.HTTPError as e:
        print(e)
        exit(-1)
    except Exception as e:
        print(e)
        exit(-2)

    xiaoqu_list = soup.find_all('div', {'class': 'info'})
    for xq in xiaoqu_list:
        info_dict= {}
        info_dict.update({u'小区名称': xq.find('a').text})

        content = xq.find('div', {'class': 'positionInfo'}).renderContents().decode('utf-8').strip()
        content = ''.join(content.replace('\n', '').split())
        info=re.match(r".+>(.+)</a>.+>(.+)</a>/(.+)", content)

        if info:
            info = info.groups()
            info_dict.update({u'大区域': info[0]})
            info_dict.update({u'小区域': info[1]})
            info_dict.update({u'建造时间': info[2]})
        command = gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command, 1)

def do_xiaoqu_spider(db_xq,region='杨浦'):
    """
       爬取大区域中的所有小区信息
       """
    region_pinyin = ''.join(lazy_pinyin(region))
    url = 'https://sh.lianjia.com/xiaoqu/' + region_pinyin + '/'
    try:
        request = urllib.request.Request(url, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(request, timeout=8)
        soup = BeautifulSoup(source_code, features='html.parser')
    except urllib.request.HTTPError as e:
        print(e)
        return
    except Exception as e:
        print(e)
        return
    d = soup.find('div', {'class': 'page-box house-lst-page-box'}).get('page-data')
    d = ast.literal_eval(d)
    total_pages=d['totalPage']
    print('————-----总页数%s--------' % total_pages)
    threads=[]
    for i in range(0, total_pages):
        url_page = u"https://sh.lianjia.com/xiaoqu/%s/pg%d/" % (region_pinyin, i+1)
        t = threading.Thread(target=xiaoqu_spider, args=(db_xq, url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(u'爬下了%s区的全部的小区信息' % region)


def chengjiao_spider(db_cj, url_page=u"https://sh.lianjia.com/chengjiao/rs%E9%94%A6%E7%BB%A3%E6%B1%9F%E5%8D%97/"):
    """
        爬取页面链接中的成交记录
        url_page: https://sh.lianjia.com/chengjiao/rs%E9%94%A6%E7%BB%A3%E6%B1%9F%E5%8D%97/
        """
    try:
        request = urllib.request.Request(url_page, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(request, timeout=8).read()
        soup = BeautifulSoup(source_code, features='html.parser')
    except urllib.request.HTTPError as e:
        print(e)
        return
    except Exception as e:
        print(e)
        return

    cj_list = soup.findAll('div', {'class':'info'})
    for cj in cj_list:
        info_dict = {}
        href=cj.find('a')
        if not href:
            continue
        info_dict.update({u'链接':href.attrs['href']})
        content= href.text.split()
        if content:
            info_dict.update({u'小区名称': content[0]})
            info_dict.update({u'户型': content[1]})
            info_dict.update({u'面积': content[2]})

        content = cj.find('div', {'class': 'houseInfo'}).getText().strip()
        # content = ''.join(content.replacecontent('\n', '').split())
        content = content.split('|')
        if content:
            info_dict.update({u'朝向':content[0].strip()})    
            info_dict.update({u'装修':content[1].strip()})

        content = cj.find('div', {'class': 'positionInfo'}).getText().strip()
        content = content.split(' ')
        if content:
            info_dict.update({u'楼层':content[0].strip()})
            info_dict.update({u'建造时间':content[1].strip()})

        content = cj.find('div', {'class': 'dealDate'}).getText().strip()
        if content:
            info_dict.update({u'签约时间':content})

        content = cj.find('div', {'class': 'totalPrice'}).getText().strip()
        if content:
            info_dict.update({u'签约总价': content})

        content = cj.find('div', {'class': 'unitPrice'}).getText().strip()
        if content:
            info_dict.update({u'签约单价': content})

        print(info_dict)
        command = gen_chengjiao_insert_command(info_dict)
        db_cj.execute(command, 1)

def xiaoqu_chengjiao_spider(db_cj,xq_name=u"锦绣江南"):
    """
    爬取小区成交记录
    """
    url = u"http://sh.lianjia.com/chengjiao/rs" + urllib.request.quote(xq_name) + "/"
    try:
        request = urllib.request.Request(url, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib.request.urlopen(request, timeout=8).read()
        soup = BeautifulSoup(source_code, features='html.parser')
    except urllib.request.HTTPError  as e:
        print(e)
        exception_write('xiaoqu_chengjiao_spider', xq_name)
        return
    except Exception as e:
        print(e)
        exception_write('xiaoqu_chengjiao_spider', xq_name)
        return

    content = soup.find('div', {'class': 'page-box house-lst-page-box'})
    total_pages=0
    if content:
        d = content.get('page-data')
        d = ast.literal_eval(d)
        total_pages = d['totalPage']

    threads = []
    for i in range(0,total_pages):
        url_page = u"http://sh.lianjia.com/chengjiao/pg%drs%s/" % (i + 1, urllib.request.quote(xq_name))
        t = threading.Thread(target=chengjiao_spider, args=(db_cj, url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def do_xiaoqu_chengjiao_spider(db_xq,db_cj):
    """
       批量爬取小区成交记录
       """
    count = 0
    xq_list = db_xq.fetchall()
    for xq in xq_list:
        xiaoqu_chengjiao_spider(db_cj, xq[0])
        count += 1
        print ('have spidered %d xiaoqu' % count)
    print('done')


def exception_write(fun_name,url):
    """
      写入异常信息到日志
      """
    lock.acquire()
    f = open('log.txt', 'a')
    line = "%s %s\n" % (fun_name, url)
    f.write(line)
    f.close()
    lock.release()

def exception_read():
    """
    从日志中读取异常信息
    """
    lock.acquire()
    f = open('log.txt', 'r')
    lines = f.readlines()
    f.close()
    f = open('log.txt', 'w')
    f.truncate()
    f.close()
    lock.release()
    return lines

def exception_spider(db_cj):
    """
    重新爬取爬取异常的链接
    """
    count = 0
    excep_list = exception_read()
    while excep_list:
        for excep in excep_list:
            excep = excep.strip()
            if excep == "":
                continue
            excep_name, url = excep.split(" ", 1)
            if excep_name == "chengjiao_spider":
                chengjiao_spider(db_cj, url)
                count += 1
            elif excep_name == "xiaoqu_chengjiao_spider":
                xiaoqu_chengjiao_spider(db_cj, url)
                count += 1
            else:
                print("wrong format")
            print("have spidered %d exception url" % count)
        excep_list = exception_read()
    print('all done ^_^')

if __name__ == '__main__':
    command = "create table if not exists xiaoqu (name TEXT primary key UNIQUE, regionb TEXT, regions TEXT, year TEXT)"
    db_xq = SQLiteWraper('lianjia-xq.db', command)
    command = "create table if not exists chengjiao (href char(50)  primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, fitment TEXT, floor TEXT, year TEXT, sign_time TEXT, unit_price TEXT, total_price TEXT)"
    db_cj = SQLiteWraper('lianjia-cj.db', command)

    # 爬下所有的小区信息
    for region in regions:
        do_xiaoqu_spider(db_xq, region)
    # 爬下所有小区里的成交信息z`
    do_xiaoqu_chengjiao_spider(db_xq, db_cj)
    # # 重新爬取爬取异常的链接
    exception_spider(db_cj)