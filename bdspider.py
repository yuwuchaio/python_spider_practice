from urllib.request import urlopen
from urllib.request import HTTPError
from bs4 import BeautifulSoup
import re

class Tool:
    #删除图片标签
    removeImg = re.compile('<img.*?>| {7}|')
    #删除超链接标签
    removeAddr = re.compile('<a.*?>|</a>')
    #换行标签换位\n
    replaceLine = re.compile('<tr>|<div>|</div>|</p>')
    # 将表格制表<td>替换为\t
    replaceTD = re.compile('<td>')
    # 把段落开头换为\n加空两格
    replacePara = re.compile('<p.*?>')
    # 将换行符或双换行符替换为\n
    replaceBR = re.compile('<br><br>|<br>')
    # 将其余标签剔除
    removeExtraTag = re.compile('<.*?>')
    def replace(self, x):
        x = re.sub(self.removeImg, "", x)
        x = re.sub(self.removeAddr, "", x)
        x = re.sub(self.replaceLine, "\n", x)
        x = re.sub(self.replaceTD, "\t", x)
        x = re.sub(self.replacePara, "\n    ", x)
        x = re.sub(self.replaceBR, "\n", x)
        x = re.sub(self.removeExtraTag, "", x)
        # strip()将前后多余内容删除
        return x.strip()

class BDSpider(object):

    def __init__(self, baseUrl, seeLZ, floorFlag= 0):
        self.baseUrl = baseUrl
        self.seeLZ = '?see_lz='+str(seeLZ)
        self.defaultTitle = "百度贴吧"
        self.tool = Tool()
        self.file = None
        #楼层初始层号
        self.floor = 1
        self.floorFlag = floorFlag

    # 获取贴子的标题
    def getTitle(self, page):
        pattern = re.compile('<h3 class="core_title_txt .*?>(.*?)</h3>', re.S)
        result = re.search(pattern, page.read().decode('utf-8'))
        if result:
            # 如果存在，则返回标题
            return result.group(1).strip()
        else:
            return None

    #获取帖子某页内容
    def getpage(self, pageNum):
        try:
            url = self.baseUrl + self.seeLZ + '&pn=' + str(pageNum)
            html = urlopen(url)
            return html
        except HTTPError as e:
            print('访问百度贴吧失败原因', e)

    #获取帖子的总页数
    def getPageNum(self, page):
        pattern = re.compile('<li class="l_reply_num.*?</span>.*?<span.*?>(.*?)</span>', re.S)
        result = re.search(pattern, page.read().decode('utf-8'))
        if result:
            return result.group(1).strip()
        else:
            return None

    #获取帖子的征文
    def getContent(self, page):
        pattern = re.compile('<div id="post_content_.*?>(.*?)</div>', re.S)
        items = re.findall(pattern, page.read().decode('utf-8'))
        contents = []
        for item in items:
            # 将文本进行去除标签处理，同时在前后加入换行符
            content = "\n" + self.tool.replace(item) + "\n"
            contents.append(content)
        return contents

    #写入帖子的标题
    def setFileTitle(self, title):
        if title is not None:
            self.file = open(title + ".txt", "w+")
        else:
            self.file = open(self.defaultTitle + ".txt", "w+")

    #写入帖子每楼的信息
    def writeData(self, items):
        for item in items:
            #楼之间添加分割
           if self.floorFlag == '1':
             floorLine = "\n" + str(self.floor) + u"------------------------------------------------------------------------------------------------------------------------------------\n"
             self.file.write(floorLine)
             self.file.write(item)
             self.floor += 1

    #开始
    def start(self):
        indexPage = self.getpage(1)       #获取当前页面内容
        # 总页码
        pageNum = self.getPageNum(indexPage)     #获取总页码
        #获取文章标题
        title = self.getTitle(indexPage)
        #写入标题
        self.setFileTitle(title)
        if pageNum == None:
            print("URL 失效，请重试")
            return

        try:
            print("该帖子总共有" + str(pageNum) + "页")
            for i in range(1, int(pageNum)+1):
                print("正在写入第" + str(i) + "页数据")
                page = self.getpage(i)
                contents = self.getContent(page)
                self.writeData(contents)
        except IOError as e:
            print("写入异常，原因" + e.message)
        finally:
            print("写入完成")





print("请输入帖子代号")
baseURL = 'http://tieba.baidu.com/p/' + str(input(u'http://tieba.baidu.com/p/'))
seeLZ = input("是否只获取楼主发言，是输入1，否输入0\n")
floorTag = input("是否写入楼层信息，是输入1，否输入0\n")
spider = BDSpider(baseURL.strip(), seeLZ, floorTag)
spider.start()