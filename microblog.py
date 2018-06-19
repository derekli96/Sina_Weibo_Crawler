#-*- coding:utf-8 -*-

"""
爬取新浪微博的用户信息
功能：爬取用户ID 用户名 粉丝数 关注数 微博数 性别 所在地 个人简介 微博内容 用户头像
数据保存：个人信息、微博文本保存到数据库和本地csv 头像保存到本地
网址：微博手机版 www.weibo.cn
"""
import time
import re
import os
import sys
import codecs
import shutil
import urllib
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import selenium.webdriver.support.ui as ui
from selenium.webdriver.common.action_chains import ActionChains
import pymysql
import csv
import configure
import multiprocessing
#先调用无界面浏览的浏览器 PhantomJS 或Firefox

driver=webdriver.Firefox()
wait=ui.WebDriverWait(driver,10)
stdi,stdo,stde=sys.stdin,sys.stdout,sys.stderr 
reload(sys)
#通过import引用进来时,setdefaultencoding函数在被系统调用后被删除了，所以必须reload一次
sys.stdin,sys.stdout,sys.stderr=stdi,stdo,stde 
sys.setdefaultencoding('utf-8')


def LoginWeibo(username,password):
    try:
        print u'准备登录微博...'
        driver.get("https://weibo.com/login.php") #打开登录页面
        elem_user=driver.find_element_by_name("username") #选中用户名输入框
        elem_user.send_keys(username) #填入用户名
        elem_pwd=driver.find_element_by_name("password") #选中密码输入框
        elem_pwd.send_keys(password) #填入密码
        elem_sub=driver.find_element_by_xpath('//a[@tabindex=6]')
        elem_sub.click() #点击登录
        
        #暂停时间输入验证码
        time.sleep(15)

        print u'微博登录成功！'
    except Exception,e:
        print "错误：",e
    finally:
        print u'微博登录结束！\n'

        
def VisitPersonPage(user_id, cursor, connection, writer_info, writer_wb):
    try:
        print u'准备进入个人网站...'
        driver.get("https://weibo.cn/"+user_id) #跳转到个人主页
        time.sleep(2)
        
        #第一步：直接获取用户昵称/微博数/关注数/粉丝数

        #用户id
        print u'准备访问个人信息...'
        print u'用户id:'+user_id

        #昵称
        str_name=driver.find_element_by_xpath("//div[@class='ut']")
        str_t=str_name.text.split(" ")
        num_name=str_t[0]
        print u'昵称:'+num_name
        sql_name = "UPDATE `microblog`.`user_profile` SET `user_name` = '%s' WHERE `user_href` = '%s'" % (num_name,user_id)
        cursor.execute(sql_name)
        connection.commit()

        #头像
        str_por=driver.find_element_by_xpath("//img[@class='por']").get_attribute("src")
        print u'头像url:'+str_por
        getPhoto(str_por,user_id,cursor,connection)

        #微博数 除了个人主页外 它默认直接显示微博数 无超链接
        str_wb=driver.find_element_by_xpath("//div[@class='tip2']")
        pattern=r"\d+\.?\d*"  #正则提取"微博[0]" 但r"(\[.*?\])"总含[]
        guid=re.findall(pattern,str_wb.text,re.S|re.M)
        print str_wb.text
        for value in guid:
            num_wb=int(value)
            break
        print u'微博数:'+str(num_wb)
        sql_weibo = "UPDATE `microblog`.`user_profile` SET `user_weibo` = %d WHERE `user_href` = '%s'" % (num_wb,user_id)
        cursor.execute(sql_weibo)
        connection.commit()

        #关注数
        str_gz=driver.find_element_by_xpath("//div[@class='tip2']/a[1]")
        num_gz_temp=re.findall(pattern,str_gz.text,re.S|re.M)
        num_gz=int(num_gz_temp[0])
        print u'关注数:'+str(num_gz)
        sql_follow = "UPDATE `microblog`.`user_profile` SET `user_follow` = %d WHERE `user_href` = '%s'" % (num_gz,user_id)
        cursor.execute(sql_follow)
        connection.commit()

        #粉丝数
        str_fs = driver.find_element_by_xpath("//div[@class='tip2']/a[2]")
        num_fs_temp = re.findall(pattern, str_fs.text, re.S | re.M)
        num_fs = int(num_fs_temp[0])
        print u'粉丝数:' + str(num_fs)
        sql_fan = "UPDATE `microblog`.`user_profile` SET `user_fan` = %d WHERE `user_href` = '%s'" % (num_fs,user_id)
        cursor.execute(sql_fan)
        connection.commit()

        #第二步：进入个人信息页，获取详细信息
        print '\n'
        print u'准备访问个人详细信息...'
        driver.get("https://weibo.cn/"+user_id)
        time.sleep(5)
        elem_info = driver.find_element_by_link_text(u"资料")
        elem_info.click()
        time.sleep(3)
        str_detail = driver.find_element_by_xpath("/html/body/div[6]").text
        str_d = str_detail.split("\n")
        str_d_len = len(str_d)
        for i in range (0,str_d_len):
            str_item = str_info.split(":")
            
            #性别
            if( u"性别" in str_item[0]):
                if(str_item[1]==u"女"):
                    user_sex = 'F'
                    print u'性别：' + str_item[1])
                    sql_sex = "UPDATE `microblog`.`user_profile` SET `user_sex` = 'F' WHERE `user_href` = '%s'" % user_id
                    cursor.execute(sql_sex)
                    connection.commit()
                elif(str_item[1]==u"男"):
                    user_sex = 'M'
                    print u'性别：' + str_item[1])
                    sql_sex = "UPDATE `microblog`.`user_profile` SET `user_sex` = 'M' WHERE `user_href` = '%s'" % user_id
                    cursor.execute(sql_sex)
                    connection.commit()
                    
            #所在地
            elif(u"地区" in str_item[0]):
                user_location = str_item[1]
                print u'地区：' + str_item[1]
                sql_location = "UPDATE `microblog`.`user_profile` SET `user_location` = '%s' WHERE `user_href` = '%s'" % (str(str_item[1]),user_id)
                cursor.execute(sql_location)
                connection.commit()
                
            #个人简介
            elif(u"简介" in str_item[0]):
                user_intro = str_item[1]
                print u'个人简介：' + str_item[1]
                sql_introduction = "UPDATE `microblog`.`user_profile` SET `user_introduction` = '%s' WHERE `user_href` = '%s'" % (str(str_item[1]),user_id)
                cursor.execute(sql_introduction)
                connection.commit()
        
        writer_info.writerow([user_id, num_name, str(num_wb), str(num_gz), str(num_fs), user_sex, user_location, user_intro])
        
        #第三步：获取微博内容
        driver.get("https://weibo.cn/"+user_id) #返回个人主页
        time.sleep(2)
        print '\n'
        print u'准备获取微博内容信息...'
        str_num = driver.find_element_by_xpath("//input[@name='mp']").get_attribute("value")
        total_num = int(str_num) 
        num=1
        print u'总页数：%d' % total_num #获取总页数
        
        while num <= total_num:
            url_wb='https://weibo.cn/'+user_id+"?filter=0&page="+str(num)
            time.sleep(1)
            driver.get(url_wb)
            info_temp="//div[@class='c'][{0}]"
            num_temp=1
            while True:
                info = driver.find_element_by_xpath(info_temp.format(num_temp)).text
                if u'设置:皮肤.图片' not in info:
                    #微博类型：转发微博
                    if info.startswith(u'转发'):
                        if_repost = 'T'
                        print u'转发微博'

                        #点赞数
                        #获取最后一个点赞数
                        str1 = info.split(u" 赞")[-1]
                        if str1:
                            like = re.match(r'\[(.*?)\]', str1).groups()[0]
                            print u'点赞数: ' + like

                        #转发数
                        str2 = info.split(u"转发")[-1]
                        if str2:
                            repost = re.match(r'\[(.*?)\]', str2).groups()[0]
                            print u'转发数: ' + repost

                        #评论数
                        str3 = info.split(u"评论")[-1]
                        if str3:
                            comment = re.match(r'\[(.*?)\]', str3).groups()[0]
                            print u'评论数: ' + comment

                        #发博时间
                        str4 = info.split(u"收藏 ")[-1]
                        flag = str4.find(u"来自")
                        weibo_time = str4[:flag]
                        print u'时间: ' + str4[:flag]

                        #原微博点赞数
                        str5 = info.split(u" 赞")[1]
                        if str5:
                            o_like = re.match(r'\[(.*?)\]', str5).groups()[0]
                            print u'原文点赞数: ' + o_like
                        
                        #原微博转发数
                        str6 = info.split(u"原文转发")[-1]
                        if str2:
                            o_repost = re.match(r'\[(.*?)\]', str2).groups()[0]
                            print u'原文转发数: ' + o_repost                            

                        #原微博评论数
                        str7 = info.split(u"原文评论")[-1]
                        if str3:
                            o_comment = re.match(r'\[(.*?)\]', str3).groups()[0]
                            print u'原文评论数: ' + o_comment

                        #微博内容
                        str_weibo = info[info.index(u"转发理由"):info.rindex(u" 赞")]
                        print u'微博内容:' + info[info.index(u"转发理由"):info.rindex(u" 赞")]  #后去最后一个赞位置
                        print '\n'

                        #原微博内容
                        str_oweibo = info[info.index(u"微博"):info.index(u" 赞")]
                        print u'原微博内容:' + info[info.index(u"微博"):info.index(u" 赞")]  # 后去最后一个赞位置
                        print '\n'

                        sql_weibo = "INSERT INTO `microblog`.`user_weibo` (`user_href`, `weibo_text`, `weibo_like`, `weibo_comment`, `weibo_repost`, `if_repost`, `origin_text`, `origin_like`, `origin_comment`, `origin_repost`, `time`) VALUES ('%s', '%s', '%d', '%d', '%d', '%c', '%s', '%d', '%d', '%d', '%s')" % (user_id, str_weibo, int(like), int(comment), int(repost), 'T', str_oweibo, int(o_like), int(o_comment), int(o_repost), weibo_time)
                        cursor.execute(sql_weibo)
                        connection.commit()
                        writer_wb.writerow([user_id, str_weibo, like, comment, repost, 'T', str_oweibo, o_like, o_comment,o_repost, weibo_time])

                    else:
                        if_repost = 'F'
                        print u'原创微博'

                        #获取最后一个点赞数 因为转发是后有个点赞数
                        #点赞数
                        str1 = info.split(u" 赞")[-1]
                        if str1:
                            like = re.match(r'\[(.*?)\]', str1).groups()[0]
                            print u'点赞数: ' + like

                        #转发数
                        str2 = info.split(u"转发")[-1]
                        if str2:
                            repost = re.match(r'\[(.*?)\]', str2).groups()[0]
                            print u'转发数: ' + repost
                        
                        #评论数
                        str3 = info.split(u"评论")[-1]
                        if str3:
                            comment = re.match(r'\[(.*?)\]', str3).groups()[0]
                            print u'评论数: ' + comment

                        #发博时间
                        str4 = info.split(u"收藏 ")[-1]
                        flag = str4.find(u"来自")
                        weibo_time = str4[:flag]
                        print u'时间: ' + str4[:flag]
                        
                        #微博内容
                        str_weibo = info[:info.rindex(u" 赞")]
                        print u'微博内容:' + info[:info.rindex(u" 赞")]  # 后去最后一个赞位置
                        print '\n'

                        sql_weibo = "INSERT INTO `microblog`.`user_weibo` (`user_href`, `weibo_text`, `weibo_like`, `weibo_comment`, `weibo_repost`, `if_repost`, `time`) VALUES ('%s', '%s', '%d', '%d', '%d', '%c', '%s')" % (user_id, str_weibo, int(like), int(comment), int(repost), 'F', weibo_time)
                        cursor.execute(sql_weibo)
                        connection.commit()

                        writer_wb.writerow([user_id, str_weibo, like, comment, repost, 'F', '', '0', '0', '0', weibo_time])
                    
                else:
                    print u'完成本页微博爬取', '\n'
                    break

                num_temp+=1
            num+=1
    
    except Exception, e:
        print "错误: ", e
    finally:
        print u'完成爬取 %s 的信息' % user_id
        print '**********************************************\n'

def getPhoto(addr,user_id,cursor,connection):
    web = urllib.urlopen(addr)
    itdata = web.read()
    fsave = open('d:/weibo/'+user_id+'.png',"wb")
    fsave.write(itdata)
    fsave.close()
    print u'成功下载头像！'

if __name__ == '__main__':

    #定义变量
    username = "username"  #输入用户名
    password = "password"  #输入密码
    
    #登录
    LoginWeibo(username, password)   #登录微博
    print u'准备连接数据库...'
    connection=pymysql.connect(host='127.0.0.1', port=3306, user="root", passwd="root", db='microblog', charset="utf8", cursorclass=pymysql.cursors.DictCursor)
    print u'成功连接数据库！'
    cursor=connection.cursor()
    print u'读取用户id...'
    
    sql_user = 'SELECT user_href FROM microblog.user_profile' #从数据库读取需要爬取的用户的id
    cursor.execute(sql_user)
    result = cursor.fetchall()
    result_len = len(result)
    print u'准备爬取...'
    
    #建立储存个人信息的csv文件
    csvfile_info = file('user_info.csv', 'wb')
    writer_info = csv.writer(csvfile_info)
    writer_info.writerow(['user_href', 'user_name', 'user_weibo', 'user_follow', 'user_fan', 'user_sex', 'user_location', 'user_intro'])
    
    #建立储存微博文本的csv文件
    csvfile_wb = file('weibo_content.csv', 'wb')
    writer_wb = csv.writer(csvfile_wb)
    writer_wb.writerow(['user_href', 'weibo_text', 'weibo_like', 'weibo_comment', 'weibo_repost', 'if_repost', 'origin_text', 'origin_like', 'origin_comment', 'origin_repost', 'time'])

    #开始爬取信息
    for i in range(0,result_len):    
        user_id=result[i]['user_href']
        user_id=user_id.rstrip('\r\n')
        VisitPersonPage(user_id, cursor, connection, writer_info, writer_wb)

    cursor.close()
    connection.close()
