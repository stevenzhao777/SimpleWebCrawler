__author__ = 'xuanhaozhao'

import urllib
import urllib2
from bs4 import BeautifulSoup
import pymysql
import re
import threading
import time

linkCond=threading.Condition()
pageCond=threading.Condition()

def newLinkTask(conn,cur):
    try:
        statement='''SELECT COUNT(*) FROM CNNLink WHERE type="link" AND state="new"'''
        cur.execute(statement)
    except pymysql.MySQLError as e:
        print "Error in function newLinkTask"
        print e.args
        raise pymysql.MySQLError
    else:
        return cur.fetchone()[0]>0

def newPageTask(conn,cur):
    try:
        statement='''SELECT COUNT(*) FROM CNNLink WHERE type="page" AND state="new"'''
        cur.execute(statement)
    except pymysql.MySQLError as e:
        print "Error in function newPageTask"
        print e.args
        raise pymysql.MySQLError
    else:
        return cur.fetchone()[0]>0



def getCNNLink(conn,cur):
    url=None
    with linkCond:
        try:
            while not newLinkTask(conn,cur):
                linkCond.wait()
        except:
            return
        else:
            try:
                statement='''SELECT link FROM CNNLink WHERE type="link" AND state="new"'''
                cur.execute(statement)
                url=cur.fetchone()[0]
                url=url[1:-1]
                statement='''UPDATE CNNLink SET state="working" WHERE link="%s" and type="link"'''
                cur.execute(statement,url)
                conn.commit()
            except pymysql.MySQLError as e:
                print "Error in function getCNNLink"
                print e.args
                return

    time.sleep(1)

    try:
         html=urllib2.urlopen(url)
    except urllib2.URLError as e:
        print "Exception in function getCNNLink"
        print e.args
        return
    bsObj=BeautifulSoup(html,"html.parser")

    try:
        try:
            getCNNGeneralLink(url,bsObj,conn,cur)
        except pymysql.MySQLError as e:
            print "Error in getCNNGeneralLink"
            print e.args
        try:
            getCNNPageLink(url,bsObj,conn,cur)
        except pymysql.MySQLError as e:
            print "Error in getCNNPageLink"
            print e.args
    except Exception as e:
        print "Error in function getCNNLink"
        print e.args
    else:
        with linkCond:
            try:
                statement='''UPDATE CNNLink SET state="done" WHERE link="%s" AND type="link"'''
                cur.execute(statement,url)
                conn.commit()
            except pymysql.MySQLError as e:
                print "Error in function getCNNLink"
                print e.args


def getCNNPage(conn,cur):
    url=None
    with pageCond:
        try:
            while not newPageTask(conn,cur):
                pageCond.wait()
        except:
            return
        else:
            try:
                statement='''SELECT link FROM CNNLink WHERE type="page" AND state="new"'''
                cur.execute(statement)
                url=cur.fetchone()[0]
                url=url[1:-1]
                statement='''UPDATE CNNLink SET state="working" WHERE link="%s" AND type="page"'''
                cur.execute(statement,url)
                conn.commit()
            except pymysql.MySQLError as e:
                print "Error in function getCNNPage"
                print e.args
                return

    time.sleep(1)

    try:
         html=urllib2.urlopen(url)
    except urllib2.URLError as e:
        print "url error"
        print e.args
        return
    bsObj=BeautifulSoup(html,"html.parser")


    author=bsObj.find("meta",{"name":"author"})
    title=bsObj.find("title")
    section=bsObj.find("meta",{"name":"section"})
    pubdate=bsObj.find("meta",{"name":"pubdate"})
    lastmod=bsObj.find("meta",{"name":"lastmod"})
    description=bsObj.find("meta",{"name":"description"})

    articleTags=bsObj.findAll("p",{"class":"zn-body__paragraph"})

    if author is None or title is None or section is None or pubdate is None or lastmod is None or description is None or articleTags is None:
        try:
            statement='''UPDATE CNNLinks SET state="invalid" WHERE url="%s" AND type="page"'''
            cur.execute(statement,url)
            conn.commit()
        except pymysql.MySQLError as e:
            print "url not a valid page url"
            print e.args
        finally:
            return

    author=author["content"]
    title=title.get_text()
    section=section["content"]
    pubdate=pubdate["content"]
    lastmod=lastmod["content"]
    description=description["content"]

    article=""
    for s in articleTags:
        article+=(s.get_text()+"\n"*2)

    try:
         statement='''INSERT INTO CNNNews (title,author,section,pubdate,lastmod,url,description,article) VALUES ("%s","%s","%s","%s","%s","%s","%s","%s")'''
         cur.execute(statement,(title,author,section,pubdate,lastmod,url,description,article))
         conn.commit()

         with linkCond:
            statement='''UPDATE CNNLink SET state="done" WHERE link="%s" AND type="page"'''
            cur.execute(statement,url)
            conn.commit()
    except pymysql.MySQLError as e:
        print 'Error in getCNNPage'
        print(e.args)
         #should do logging here. which seems pretty complicated in itself. will probably just write a very simple log

def getCNNGeneralLink(url,bsObj,conn,cur):
    try:
        tags=bsObj.findAll("a",{"href":re.compile("^(http://www\.cnn\.com)?/.*")})
        tags=set(tags)
        with linkCond:
            for tag in tags:
                link=tag["href"]
                if not link.startswith("http://www.cnn.com"):
                    link="http://www.cnn.com"+link

                statement='''INSERT INTO CNNLink VALUES ("%s","link","new")'''
                cur.execute(statement,link)
                conn.commit()
                linkCond.notify()
    except pymysql.MySQLError as e:
        print "Error in function getCNNGeneralLink"
        print e.args
        raise pymysql.MySQLError
         #should do logging here. which seems pretty complicated in itself. will probably just write a very simple log



def getCNNPageLink(url,bsObj,conn,cur):
    try:
        tags=bsObj.findAll("a",{"href":re.compile("^(http://www\.cnn\.com)?/\d+/\d+/\d+.*")})
        tags=set(tags)
        with pageCond:
            for tag in tags:
                link=tag["href"]
                if not link.startswith("http://www.cnn.com"):
                    link="http://www.cnn.com"+link

                statement='''INSERT INTO CNNLink VALUES ("%s","page","new")'''
                cur.execute(statement,link)
                conn.commit()
                pageCond.notify()
    except pymysql.MySQLError as e:
        print "Error in function getCNNPageLink"
        print e.args
        raise pymysql.MySQLError
         #should do logging here. which seems pretty complicated in itself. will probably just write a very simple log

def initCNN(conn,cur):
    statement='''INSERT INTO CNNLink VALUES("'http://www.cnn.com'","link","new")'''
    try:
        cur.execute(statement)
        conn.commit()
    except pymysql.MySQLError as e:
        print "Cannnot initialize CNN"

def dispatchThreads(conn,cur,numOfGenLink=1,numOfPageLink=1):
    threadList=[]
    for _ in range(numOfGenLink):
        thread=threading.Thread(target=getCNNLink(conn,cur))
        thread.start()
        threadList.append(thread)
    for _ in range(numOfPageLink):
        thread=threading.Thread(target=getCNNPage(conn,cur))
        thread.start()
        threadList.append(thread)
    return threadList


if __name__=="__main__":
   try:
         conn=pymysql.connect(host='127.0.0.1',unix_socket='/tmp/mysql.sock',user='root',passwd=None,db='web_crawler',charset='utf8')
         cur=conn.cursor()
         initCNN(conn,cur)
         while True:
             threadList=dispatchThreads(conn,cur,5,5)
   except pymysql.MySQLError as e:
       print "Exception in main"
       print e.args
   finally:
       cur.close()
       conn.close()



