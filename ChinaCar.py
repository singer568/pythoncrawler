import requests
import re
from selenium import webdriver
import pymysql
import os
from math import ceil
import configparser
import time
import logging
import datetime

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename='chinacar.log', filemode="w", level=logging.DEBUG)
connection = pymysql.connect(host='127.0.0.1',
                           port=3306,
                           user='root',
                           password='yanghongsong',
                           database='craw',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()
logging.debug('数据库连接成功')

cf = configparser.ConfigParser()
cf.read('config.conf',encoding="utf-8-sig")
picihao = cf.get("chinacar", "picihao")
querymingcheng=cf.get("chinacar", "cheliangmingcheng")
frompageno = cf.get("chinacar", "frompagenum")
topagenum =  cf.get("chinacar", "topagenum")
logging.debug('得到批次号：' + picihao + ",frompagenum=" + frompageno + ',topagenum' + topagenum)
frompageno = int(frompageno)


logging.debug('准备请求第一页，获取总条数')
search_url ='http://www.chinacar.com.cn/Home/GonggaoSearch/GonggaoSearch/search_json'
query_id = {
            's4': picihao,
            's0':querymingcheng,
            'page': 1,
            'start': 0,
            'limit': 400
    }
search_response = requests.post(search_url, data=query_id)
a = search_response.text
pattern = re.compile(r"totalCount\":\"\d{1,20}");
result = pattern.findall(a)
if len(result) == 0:
    logging.debug('没有此批次的数据')
    time.sleep(9999)
    os._exit()
pages = str(result[0]).replace("\"","").replace("totalCount:", "")
pages = int(pages)
if pages <= 0 :
    logging.debug('没有此批次的数据')
    time.sleep(9999)
    os._exit()
pages = ceil(pages/400) + 1
logging.debug('得到总页数' + str(pages))
if topagenum.strip() != '' :
    pages = topagenum
for x in range(frompageno, pages):
    logging.debug('抓取第'+ str(x) + '页')
    query_id = {
            's4': picihao,
            's0': querymingcheng,
            'page': x,
            'start': (x-1)*400,
            'limit': 400
    }
    search_url ='http://www.chinacar.com.cn/Home/GonggaoSearch/GonggaoSearch/search_json'
    logging.debug(' 开始发送数据请求,获取本页所有carid')
    search_response = requests.post(search_url, data=query_id)
    a = search_response.text
    pattern = re.compile(r'\'tarid\':\'[a-zA-Z0-9]{8}\'');
    result = pattern.findall(a)
    logging.debug(' 得到本页所有carid')
    if len(result) == 0:
        logging.debug(' 本页所有carid，退出')
        time.sleep(9999)
        break
    rows =0
    for key in result:
        rows = rows + 1
        e = key.replace('\'tarid\':', '').replace('\'','')
        logging.debug('     开始抓取第' +str(x) + '页，第' + str(rows) + '条数据，carid=' + e)

        querySql = " select 'carid' from chinacar where carid = '" + e + "'"
        cursor.execute(querySql)
        queryResult = cursor.fetchall()
        if len(queryResult) != 0:
            logging.debug('     数据库已存在此数据' + e + '，略过')
            continue
        logging.debug('     数据库不存在此数据' + e + '，执行插入')


        driver = webdriver.Chrome(executable_path='C:\chromedriver.exe')
        logging.debug('     启动chromedriver')

        t = driver.get("http://www.chinacar.com.cn/ggcx_new/search_view.html?id="+e)
        logging.debug('     打开详情页面')
        ''' 生产企业信息 '''

        try:
            cheliangmingcheng = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[2]/td[2]/a").text
        except:
            driver.quit()
            time.sleep(2)
            driver = webdriver.Chrome(executable_path='C:\chromedriver.exe')
            logging.debug('     重新启动chromedriver')
            t = driver.get("http://www.chinacar.com.cn/ggcx_new/search_view.html?id=" + e)
            logging.debug('     重新打开详情页面')
            cheliangmingcheng = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[2]/td[2]/a").text


        cheliangleixing = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[2]/td[4]/span[1]/a").text
        zhizaodi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[3]/td[2]").text
        paizhaoleixing = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[3]/td[4]/span/a").text
        gonggaopici = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[4]/td[2]").text
        faburiqi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[4]/td[4]").text
        chanpinhao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[5]/td[2]/span").text
        muluxuhao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[5]/td[4]").text
        zhongwenpinpai = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[6]/td[2]/a").text
        yingwenpinpai = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[6]/td[4]").text
        gonggaoxinghao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[7]/td[2]").text
        mianzheng = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[7]/td[4]").text
        qiyemingcheng = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[8]/td[2]").text
        ranyou = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[8]/td[4]").text
        qiyedizhi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[9]/td[2]").text
        huanbao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[9]/td[4]").text
        '''免检说明'''
        mianjian = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[11]/td[2]").text
        mianjianyouxiaoqi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[11]/td[4]").text
        '''公告状态'''
        gonggaozhuangtai = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[13]/td[2]").text
        shengxiaoriqi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[13]/td[4]").text
        zhuangtaimiaoshu = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[14]/td[2]").text
        biangengjilu = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[14]/td[4]").text
        '''主要技术参数'''
        waixingchicun = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[16]/td[2]/span").text
        huoxiangchicun = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[16]/td[4]/span").text
        zongzhiliang = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[17]/td[2]/span").text
        liyongxishu = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[17]/td[4]").text
        zhengbeizhiliang = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[18]/td[2]/span").text
        edingzaizhiliang = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[18]/td[4]/span").text
        guachezhiliang = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[19]/td[2]").text
        banguaanzuo = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[19]/td[4]").text
        jiashishi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[20]/td[2]").text
        qianpaichengke = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[20]/td[4]").text
        edingzaike = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[21]/td[2]").text
        fangbaosi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[21]/td[4]").text
        jiejinjiao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[22]/td[2]").text
        qianxuan = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[22]/td[4]/span").text
        zhouhe = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[23]/td[2]").text
        zhouju = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[23]/td[4]/span").text
        zhoushu = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[24]/td[2]").text
        zuigaochesu = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[24]/td[4]").text
        youhao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[25]/td[2]").text
        tanhuangpianshu = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[25]/td[4]").text
        luntaishu = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[26]/td[2]").text
        luntaiguige = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[26]/td[4]").text
        qianlunju = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[27]/td[2]/span").text
        houlunju = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[27]/td[4]/span").text
        zhidongqian = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[28]/td[2]").text
        zhidonghou = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[28]/td[4]").text
        zhicaoqian = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[29]/td[2]").text
        zhicaohou = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[29]/td[4]").text
        zhuanxiangxingshi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[30]/td[2]").text
        qidongfangshi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[30]/td[4]").text
        chuandongxingshi = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[31]/td[2]").text
        youhao2 = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[31]/td[4]").text
        shibiedaima = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[32]/td[2]/span[1]").text
        '''发动机参数'''
        fadongji = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[35]/td[1]").text
        shengchanqiye = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[35]/td[2]").text
        pailiang = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[35]/td[3]").text
        gonglv = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[35]/td[4]").text

        '''新能源标记'''
        biaoji = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[36]/td/b").text
        if biaoji == '新能源参数':
            '''车辆燃料参数'''
            ranliaozhonglei = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[40]/td[2]").text
            yijubiaozhun = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[40]/td[4]/span").text
            paifangbiaozhun = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[41]/td[2]").text
            qita = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[43]/td").text
            '''反光标识参数'''
            biaoshiqiye = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[45]/td[2]").text
            biaoshishangbiao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[45]/td[4]").text
            biaoshixinghao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[46]/td[2]").text
        else:
            '''车辆燃料参数'''
            ranliaozhonglei = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[37]/td[2]").text
            yijubiaozhun = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[37]/td[4]/span").text
            paifangbiaozhun = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[38]/td[2]").text
            qita = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[40]/td").text
            '''反光标识参数'''
            biaoshiqiye = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[42]/td[2]").text
            biaoshishangbiao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[42]/td[4]").text
            biaoshixinghao = driver.find_element_by_xpath("//*[@id=\"con_two_p1\"]/table/tbody/tr[43]/td[2]").text

        driver.quit()
        logging.debug('     得到所有数据，关闭chromedriver，开始执行插入操作')
        d = datetime.datetime.now()
        insertSql = "INSERT INTO `chinacar` (`carid`, `pihao`,`crawdate`,`querymingcheng`, `cheliangmingcheng`, `cheliangleixing`, `zhizaodi`, `paizhaoleixing`, `gonggaopici`, `faburiqi`, `chanpinhao`, `muluxuhao`, `zhongwenpinpai`, `yingwenpinpai`, `gonggaoxinghao`, `mianzheng`, `qiyemingcheng`, `ranyou`, `qiyedizhi`, `huanbao`, `mianjian`, `mianjianyouxiaoqi`, `gonggaozhuangtai`, `shengxiaoriqi`, `zhuangtaimiaoshu`, `biangengjilu`, `waixingchicun`, `huoxiangchicun`, `zongzhiliang`, `liyongxishu`, `zhengbeizhiliang`, `edingzaizhiliang`, `guachezhiliang`, `banguaanzuo`, `jiashishi`, `qianpaichengke`, `edingzaike`, `fangbaosi`, `jiejinjiao`, `qianxuan`, `zhouhe`, `zhouju`, `zhoushu`, `zuigaochesu`, `youhao`, `tanhuangpianshu`, `luntaishu`, `luntaiguige`, `qianlunju`, `houlunju`, `zhidongqian`, `zhidonghou`, `zhicaoqian`, `zhicaohou`, `zhuanxiangxingshi`, `qidongfangshi`, `chuandongxingshi`, `youhao2`, `shibiedaima`, `fadongji`, `shengchanqiye`, `pailiang`, `gonglv`, `ranliaozhonglei`, `yijubiaozhun`, `paifangbiaozhun`, `qita`, `biaoshiqiye`, `biaoshishangbiao`, `biaoshixinghao`) VALUES ( '%s', '%s','%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (e, picihao,d,querymingcheng, cheliangmingcheng, cheliangleixing, zhizaodi, paizhaoleixing, gonggaopici, faburiqi, chanpinhao, muluxuhao, zhongwenpinpai, yingwenpinpai, gonggaoxinghao, mianzheng, qiyemingcheng, ranyou, qiyedizhi, huanbao, mianjian, mianjianyouxiaoqi, gonggaozhuangtai, shengxiaoriqi, zhuangtaimiaoshu, biangengjilu, waixingchicun, huoxiangchicun, zongzhiliang, liyongxishu, zhengbeizhiliang, edingzaizhiliang, guachezhiliang, banguaanzuo, jiashishi, qianpaichengke, edingzaike, fangbaosi, jiejinjiao, qianxuan, zhouhe, zhouju, zhoushu, zuigaochesu, youhao, tanhuangpianshu, luntaishu, luntaiguige, qianlunju, houlunju, zhidongqian, zhidonghou, zhicaoqian, zhicaohou, zhuanxiangxingshi, qidongfangshi, chuandongxingshi, youhao2, shibiedaima, fadongji, shengchanqiye, pailiang, gonglv, ranliaozhonglei, yijubiaozhun, paifangbiaozhun, qita, biaoshiqiye, biaoshishangbiao, biaoshixinghao)
        try:
            # 执行sql语句
            cursor.execute(insertSql)
            # 提交到数据库执行
            connection.commit()
        except Exception as exp:
            raise Exception(exp)
            connection.rollback()
        logging.debug('     本条数据插入完毕')

print('数据抓取完毕，总计pages：' + str(pages))
time.sleep(9999)

