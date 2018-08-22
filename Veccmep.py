import requests
import pymysql
from lxml import etree
import configparser
from selenium import webdriver
import logging
import time

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename='veccmep.log', filemode="w", level=logging.DEBUG)

connection = pymysql.connect(host='127.0.0.1',
                           port=3306,
                           user='root',
                           password='yanghongsong',
                           database='craw',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()
logging.debug('建立数据库连接')
cf = configparser.ConfigParser()
cf.read('config.conf')
frompagenum = cf.get("veccmep", "frompagenum")
topagenum = cf.get("veccmep", "topagenum")
rows = cf.get("veccmep", "rows")
param = cf.get("veccmep", "param")

logging.debug("frompagenum="+ frompagenum + "页数【" + topagenum + "】每页行数【" + rows + "】查询参数【" +param + "】")
topagenum = int(topagenum)
frompagenum = int(frompagenum)
rows = int(rows)
for x in range(frompagenum, topagenum):
    logging.debug('执行第'+ str(x) + '页数据抓取')
    query_id = {
            'param': param,
            'page': x
    }
    search_url ='http://ids.vecc-mep.org.cn/Ids/op/index.jsp'
    search_response = requests.get(search_url, params=query_id).content.decode('gb2312')
    dom_tree = etree.HTML(search_response)
    carUrl = "http://ids.vecc-mep.org.cn/Ids/op/do_Show.jsp"

    xxgkhList = dom_tree.xpath("//*[@id=\"xxgkh\"]")
    sbbhList = dom_tree.xpath("//*[@id=\"sbbh\"]")
    fdjhzhList = dom_tree.xpath("//*[@id=\"fdjhzh\"]")
    num = len(xxgkhList)
    for n in range(0, num):
        xxgkh = xxgkhList[n].get("value")
        sbbh = sbbhList[n].get("value")
        fdjhzh = fdjhzhList[n].get("value")
        logging.debug(' 执行第' + str(n) + '行数据抓取，得到的xxgkh=' + xxgkh + ',sbbh=' + sbbh + ',fdjhzh=' + fdjhzh)
        querySql = " select 'xxgkh','sbbh' from veccmep where xxgkh = '" + xxgkh + "' and sbbh='" + sbbh + "'"
        cursor.execute(querySql)
        queryResult = cursor.fetchall()
        if len(queryResult) != 0:
            logging.debug(' xxgkh=' + xxgkh + ',sbbh=' + sbbh + '的数据存在，过滤')
            continue
        logging.debug(' xxgkh=' + xxgkh + ',sbbh=' + sbbh + '的数据不存在，插入')
        logging.debug(' 启动chromedriver')

        driver = webdriver.Chrome(executable_path='C:\chromedriver.exe')
        logging.debug(' 请求详情网址')
        driver.get("http://ids.vecc-mep.org.cn/Ids/op/do_Show.jsp?xxgkh=" + xxgkh + "&sbbh="+sbbh+"&fdjhzh="+fdjhzh)
        try:
            cheliangxinghao = driver.find_element_by_xpath("/html/body/div/table[1]/tbody/tr[1]/td[2]/p/span").text
        except:
            driver.quit()
            time.sleep(2)
            driver = webdriver.Chrome(executable_path='C:\chromedriver.exe')
            logging.debug(' 请求详情网址')
            driver.get("http://ids.vecc-mep.org.cn/Ids/op/do_Show.jsp?xxgkh=" + xxgkh + "&sbbh=" + sbbh + "&fdjhzh=" + fdjhzh)
            cheliangxinghao = driver.find_element_by_xpath("/html/body/div/table[1]/tbody/tr[1]/td[2]/p/span").text

        shangbiao = driver.find_element_by_xpath("/html/body/div/table[1]/tbody/tr[2]/td[2]/p/span").text
        qichefenlei = driver.find_element_by_xpath("/html/body/div/table[1]/tbody/tr[3]/td[2]/p/span").text
        shibiefangfa = driver.find_element_by_xpath("/html/body/div/table[1]/tbody/tr[4]/td[2]/p/span").text
        zhizaoshangmingcheng = driver.find_element_by_xpath("/html/body/div/table[1]/tbody/tr[5]/td[2]/p/span").text
        shengchandizhi = driver.find_element_by_xpath("/html/body/div/table[1]/tbody/tr[6]/td[2]/p/span").text
        dianjishengchanchang = driver.find_element_by_xpath("/html/body/div/table[3]/tbody/tr[1]/td[2]/p/span").text
        zhengcheshengchanchang = driver.find_element_by_xpath("/html/body/div/table[3]/tbody/tr[2]/td[2]/p/span").text
        chunengshengchanchang = driver.find_element_by_xpath("/html/body/div/table[3]/tbody/tr[3]/td[2]/p/span").text
        dianchirongliang = driver.find_element_by_xpath("/html/body/div/table[3]/tbody/tr[4]/td[2]/p/span").text
        logging.debug(' 数据解析完毕，退出')
        driver.quit()
        logging.debug(' 执行数据插入')

        insertSql = "INSERT INTO `veccmep` (`xxgkh`, `sbbh`, `cheliangxinghao`, `shangbiao`, `qichefenlei`, `shibiefangfa`, `zhizaoshangmingcheng`, `shengchandizhi`, `yijubiaozhun`, `jiancejigou`, `jiancejielun`, `dianjishengchanchang`, `zhengcheshengchanchang`, `chunengshengchanchang`, `dianchirongliang`) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"%(xxgkh, sbbh, cheliangxinghao, shangbiao, qichefenlei, shibiefangfa, zhizaoshangmingcheng, shengchandizhi, yijubiaozhun, jiancejigou, jiancejielun, dianjishengchanchang, zhengcheshengchanchang, chunengshengchanchang, dianchirongliang)
        try:
            # 执行sql语句
            cursor.execute(insertSql)
            # 提交到数据库执行
            connection.commit()
        except Exception as exp:
            raise Exception(exp)
            time.sleep(9999)
            connection.rollback()
        logging.debug('     xxgkh=' + xxgkh + ',sbbh=' + sbbh + ',fdjhzh=' + fdjhzh +"的数据插入完成")
logging.debug('数据抓取完毕')
time.sleep(9999)