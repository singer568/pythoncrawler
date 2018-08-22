import requests
import pymysql
from lxml import etree
import time
import datetime
import configparser
import random
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', filename='miit.log', filemode="w", level=logging.INFO)

connection = pymysql.connect(host='127.0.0.1',
                           port=3306,
                           user='root',
                           password='yanghongsong',
                           database='craw',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()
logging.info('建立数据库连接')

cf = configparser.ConfigParser()
cf.read('config.conf')
pageno = cf.get("miit", "pagenum")
frompageno = cf.get("miit", "frompagenum")
pici = cf.get("miit", "pici")
logging.info('得到配置参数pageno=' + pageno + ',frompageno=' + frompageno + ',pici=' + pici)

pageno = int(pageno)
frompageno = int(frompageno)

for x in range(frompageno, pageno):
    logging.info('执行第'+ str(x) + '页数据抓取')
    query_id = {
            'categoryTreeId': 1128,
            'pagenow': x
    }
    search_url ='http://data.miit.gov.cn/resultSearch'
    search_response = requests.get(search_url, params=query_id).content.decode('utf-8')
    dom_tree = etree.HTML(search_response)
    carUrl = "http://data.miit.gov.cn/viewCar"


    for key in range(1,31):
        """
        wait = random.randint(0, 5)
        print(' 延时' + str(wait) +'s')
        time.sleep(wait)
        """
        logging.info(' 执行第' + str(key) + '行记录抓取')
        carIdHrefs = dom_tree.xpath("//*[@id=\"page-wrapper\"]/div[2]/table/tbody/tr[" + str(key) + "]/td[2]/a")
        if(len(carIdHrefs) == 0):
            logging.info(' 获取到的carId为空, 抓取完毕，退出')
            time.sleep(9999)
            break
        href = carIdHrefs[0].get("href")
        carId = href.replace("/viewCar?carId=", "")

        querySql = " select 'carid' from miit where carid = '" + carId + "'"
        cursor.execute(querySql)
        queryResult = cursor.fetchall()
        if len(queryResult) != 0:
            logging.info(' carId=' + carId + '的数据存在，过滤')
            continue
        logging.info(' carId=' + carId + '的数据不存在，插入')

        carQuery={
            'carId':carId
        }
        logging.info(' 打开详情页面')
        response1 = requests.get("http://data.miit.gov.cn/viewCar", params=carQuery).content.decode('utf-8')
        carDom = etree.HTML(response1)
        logging.info(' 获得所有数据')
        chanpinshangbiao = carDom.xpath("/html/body/div[2]/div/table[1]/tr[1]/td[1]")[0].text
        chanpinxinghao = carDom.xpath("/html/body/div[2]/div/table[1]/tr[1]/td[2]")[0].text
        chanpinmingcheng = carDom.xpath("/html/body/div[2]/div/table[1]/tr[1]/td[3]")[0].text
        qiyemingcheng = carDom.xpath("/html/body/div[2]/div/table[1]/tr[2]/td[1]")[0].text
        zhucedizhi = carDom.xpath("/html/body/div[2]/div/table[1]/tr[2]/td[2]")[0].text
        muluxuhao = carDom.xpath("/html/body/div[2]/div/table[1]/tr[3]/td[1]")[0].text
        shengchandizhi = carDom.xpath("/html/body/div[2]/div/table[1]/tr[3]/td[2]")[0].text


        waixingchicun = carDom.xpath("/html/body/div[2]/div/table[2]/tr[1]/td")[0].text
        lanbanchicun = carDom.xpath("/html/body/div[2]/div/table[2]/tr[2]/td")[0].text
        yijubiaozhun = carDom.xpath("/html/body/div[2]/div/table[2]/tr[3]/td[1]")[0].text
        ranliaozhonglei = carDom.xpath("/html/body/div[2]/div/table[2]/tr[3]/td[2]")[0].text
        zuigaochesu = carDom.xpath("/html/body/div[2]/div/table[2]/tr[4]/td[1]")[0].text
        zongzhiliang = carDom.xpath("/html/body/div[2]/div/table[2]/tr[4]/td[2]")[0].text
        liyongxishu = carDom.xpath("/html/body/div[2]/div/table[2]/tr[5]/td[1]")[0].text
        edingzhiliang = carDom.xpath("/html/body/div[2]/div/table[2]/tr[5]/td[2]")[0].text
        zhuanxiangxingshi = carDom.xpath("/html/body/div[2]/div/table[2]/tr[6]/td[1]")[0].text
        zhengbeizhiliang = carDom.xpath("/html/body/div[2]/div/table[2]/tr[6]/td[2]")[0].text
        zhoushu = carDom.xpath("/html/body/div[2]/div/table[2]/tr[7]/td[1]")[0].text
        guachezongzhiliang = carDom.xpath("/html/body/div[2]/div/table[2]/tr[7]/td[2]")[0].text
        zhouju = carDom.xpath("/html/body/div[2]/div/table[2]/tr[8]/td[1]")[0].text
        luntaiguige = carDom.xpath("/html/body/div[2]/div/table[2]/tr[8]/td[2]")[0].text
        tanhuangpianshu = carDom.xpath("/html/body/div[2]/div/table[2]/tr[9]/td[1]")[0].text
        chengzaizhiliang = carDom.xpath("/html/body/div[2]/div/table[2]/tr[9]/td[2]")[0].text
        luntaishu = carDom.xpath("/html/body/div[2]/div/table[2]/tr[10]/td[1]")[0].text
        zhunchengrenshu = carDom.xpath("/html/body/div[2]/div/table[2]/tr[10]/td[2]")[0].text
        zuoweishu = carDom.xpath("/html/body/div[2]/div/table[2]/tr[11]/td")[0].text
        zhoujuqianhou = carDom.xpath("/html/body/div[2]/div/table[2]/tr[12]/td[1]")[0].text
        jiejinjiao = carDom.xpath("/html/body/div[2]/div/table[2]/tr[12]/td[2]")[0].text
        biaoshiqiye = carDom.xpath("/html/body/div[2]/div/table[2]/tr[13]/td[1]")[0].text
        biaoshixinghao = carDom.xpath("/html/body/div[2]/div/table[2]/tr[13]/td[2]")[0].text
        biaoshishangbiao = carDom.xpath("/html/body/div[2]/div/table[2]/tr[14]/td[1]")[0].text
        fangbaosi = carDom.xpath("/html/body/div[2]/div/table[2]/tr[14]/td[2]")[0].text
        shibiedaima = carDom.xpath("/html/body/div[2]/div/table[2]/tr[15]/td[1]")[0].text
        qianhouxuan = carDom.xpath("/html/body/div[2]/div/table[2]/tr[15]/td[2]")[0].text
        qita = carDom.xpath("/html/body/div[2]/div/table[2]/tr[16]/td")[0].text
        shuoming = carDom.xpath("/html/body/div[2]/div/table[2]/tr[17]/td")[0].text
        shenbaozhi = carDom.xpath("/html/body/div[2]/div/table[2]/tr[18]/td")[0].text

        tongqishenbao = carDom.xpath("/html/body/div[2]/div/table[3]/tr[2]/td[1]")[0].text
        dipanID = carDom.xpath("/html/body/div[2]/div/table[3]/tr[2]/td[2]")[0].text
        dipanxinghao = carDom.xpath("/html/body/div[2]/div/table[3]/tr[2]/td[3]")[0].text
        dipanqiye = carDom.xpath("/html/body/div[2]/div/table[3]/tr[2]/td[4]")[0].text
        dipanleibie = carDom.xpath("/html/body/div[2]/div/table[3]/tr[2]/td[5]")[0].text

        fadongjixinghao = carDom.xpath("/html/body/div[2]/div/table[4]/tr[2]/td[1]")[0].text
        fadongjiqiye = carDom.xpath("/html/body/div[2]/div/table[4]/tr[2]/td[2]")[0].text
        pailiang = carDom.xpath("/html/body/div[2]/div/table[4]/tr[2]/td[3]")[0].text
        gonglv = carDom.xpath("/html/body/div[2]/div/table[4]/tr[2]/td[4]")[0].text
        youhao = carDom.xpath("/html/body/div[2]/div/table[4]/tr[2]/td[5]")[0].text

        logging.info(' 数据解析完毕，执行插入')
        d = datetime.datetime.now()
        insertSql = "INSERT INTO `miit` (`carid`,`pici`,`crawdate`, `chanpinshangbiao`, `chanpinxinghao`, `chanpinmingcheng`, `qiyemingcheng`, `zhucedizhi`, `muluxuhao`, `shengchandizhi`, `waixingchicun`, `lanbanchicun`, `yijubiaozhun`, `ranliaozhonglei`, `zuigaochesu`, `zongzhiliang`, `liyongxishu`, `edingzhiliang`, `zhuanxiangxingshi`, `zhengbeizhiliang`, `zhoushu`, `guachezongzhiliang`, `zhouju`, `luntaiguige`, `tanhuangpianshu`, `chengzaizhiliang`, `luntaishu`, `zhunchengrenshu`, `zuoweishu`, `zhoujuqianhou`, `jiejinjiao`, `biaoshiqiye`, `biaoshixinghao`, `biaoshishangbiao`, `fangbaosi`, `shibiedaima`, `qianhouxuan`, `qita`, `shuoming`, `shenbaozhi`, `tongqishenbao`, `dipanID`, `dipanxinghao`, `dipanqiye`, `dipanleibie`, `fadongjixinghao`, `fadongjiqiye`, `pailiang`, `gonglv`, `youhao`) VALUES ('%s','%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"%(carId,pici, d, chanpinshangbiao, chanpinxinghao, chanpinmingcheng, qiyemingcheng, zhucedizhi, muluxuhao, shengchandizhi, waixingchicun, lanbanchicun, yijubiaozhun, ranliaozhonglei, zuigaochesu, zongzhiliang, liyongxishu, edingzhiliang, zhuanxiangxingshi, zhengbeizhiliang, zhoushu, guachezongzhiliang, zhouju, luntaiguige, tanhuangpianshu, chengzaizhiliang, luntaishu, zhunchengrenshu, zuoweishu, zhoujuqianhou, jiejinjiao, biaoshiqiye, biaoshixinghao, biaoshishangbiao, fangbaosi, shibiedaima, qianhouxuan, qita, shuoming, shenbaozhi, tongqishenbao, dipanID, dipanxinghao, dipanqiye, dipanleibie, fadongjixinghao, fadongjiqiye, pailiang, gonglv, youhao)
        try:
            # 执行sql语句
            cursor.execute(insertSql)
            # 提交到数据库执行
            connection.commit()
        except Exception as exp:
            raise Exception(exp)
            connection.rollback()
        logging.info(' carId=' + carId + '的数据插入完成')
print("数据抓取完毕")
logging.info('数据抓取完毕')
time.sleep(9999)