import json
import time
import datetime
from concurrent.futures import ThreadPoolExecutor

import pymysql as py
import requests

Host = '192.168.0.187'
Port = 3306
User = 'root'
Password = '123456'
DB = 'trade'
# 时区问题，存进去+8，拿出来-8
currency = {
    "BTC": {
        "USD": {
            "optime": "2011-08-18 00:00:00",
            "tableName": "bitstamp_btcusd"
        }
    },
    "ETH": {
        "USD": {
            "optime": "2017-08-17 00:00:00",
            "tableName": "bitstamp_ethusd"
        }
    }
}
barInfo = {
    60: "1m",
    180: "3m",
    300: "5m",
    900: "15m",
    1800: "30m",
    3600: "1h",
    7200: "2h",
    14400: "4h",
    21600: "6h",
    43200: "12h",
    86400: "1d"
}

headers = {
    "Content-Type": "application/json"
}


def conn_mysql(trad1, trad2, bar):
    db = py.connect(host=Host, port=Port, user=User, password=Password,
                    database=DB)
    cur = db.cursor()
    get_restApi(db, cur, trad1, trad2, bar)

    cur.close()
    db.close()


def get_restApi(db, cursor, trad1, trad2, bar):
    beforeDate = {
        "open": 0,
        "high": 0,
        "low": 0,
        "close": 0,
        "volume": 0
    }
    interval = barInfo[bar]
    # 先是看看数据库里有没有对于bar的数据，如果没有时间就从上面的openTime开始，如果有数据，则从数据库最新的时间点开始
    tableName = currency[trad1][trad2]["tableName"]
    # query_Sql = 'select * from {} where `interval` = "{}" ORDER BY sysdatetime desc limit 1'.format(tableName,
    #                                                                                                         interval)
    query_Sql = 'select * from ' + tableName + ' where `interval` = "' + interval + '" ORDER BY sysdatetime desc limit 1'
    sql = "insert into " + tableName + "(`symbol`,`sysdatetime`,`cndatetime`,`interval`,`volume`,`open_price`,`high_price`,`low_price`,`close_price`)" \
                                       " values(%s,%s,%s,%s,%s,%s,%s,%s,%s) "\
                                       " ON DUPLICATE KEY UPDATE " \
                                       " symbol = values(symbol), " \
                                       " sysdatetime = values(sysdatetime), "\
                                       " cndatetime = values(cndatetime), " \
                                       " `interval` = values(`interval`), " \
                                       " volume = values(volume), " \
                                       " open_price = values(open_price), " \
                                       " high_price = values(high_price), " \
                                       " low_price = values(low_price), " \
                                       " close_price = values(close_price) "
    instId = "{}/{}".format(trad1, trad2)

    try:
        optime = currency[trad1][trad2]["optime"]
        optime = queryMysql(optime, cursor, query_Sql, beforeDate,bar)
        url = "https://www.bitstamp.net/api-internal/tradeview/price-history/{}/{}/".format(trad1, trad2)
        # 获取当前时间
        LocalTime = (datetime.datetime.now() + datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M") + ":10"
        while True:
            response = requests.session()
            response.adapters.DEFAULT_RETRIES = 5
            response.keep_alive = False
            # 去获取截至时间
            endTime = calculation_endtime(optime, bar)
            params = {
                "step": bar,
                "start_datetime": optime,
                "end_datetime": endTime
            }
            # 获取当前时间
            nowTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            nowTime2 = (datetime.datetime.now() + datetime.timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M") + ":10"
            if nowTime > LocalTime:
                print("准备开始请求数据，本次插入交易对：{}/{}，时间级别：{}，当前时间：{}，LocalTime:{}".format(trad1, trad2, interval, nowTime,
                                                                                   LocalTime))
                try:
                    rp = response.get(url, headers=headers, params=params)
                    data = json.loads(str(rp.text))["data"]
                    if len(data) == 0:
                        time.sleep(1)
                        nowUrl = "https://www.bitstamp.net/api-internal/market/current-candle/{}/{}/".format(trad1,trad2)
                        nowParams = {
                            "step": bar
                        }
                        rp_1 = response.get(nowUrl, headers=headers, params=nowParams)
                        nowData = json.loads(str(rp_1.text))["data"]
                        cnSqltime = datetime.datetime.fromtimestamp(int(nowData["timestamp"]))
                        sysSqltime = (cnSqltime + datetime.timedelta(hours=-8))
                        print("历史数据已全，本次历史请求无数据，开始请求最新实时数据:",bar,cnSqltime)
                        cursor.execute(sql, (
                            instId, sysSqltime, cnSqltime, interval, nowData["volume"], nowData["open"],
                            nowData["high"],
                            nowData["low"], nowData["close"]))
                    else:
                        for i in range(len(data)):
                            sysdatetime = str(data[i]['time']).replace("T", " ")
                            next_optime = datetime.datetime.strptime(sysdatetime, "%Y-%m-%d %H:%M:%S")
                            cndatetime = datetime.datetime.strptime(sysdatetime, "%Y-%m-%d %H:%M:%S")
                            cndatetime = (cndatetime + datetime.timedelta(hours=8))
                            # 如果开高低收的数据为空，就沿用之前的数据
                            if data[i]["open"] is None:
                                cursor.execute(sql, (
                                    instId, sysdatetime, cndatetime, interval, 0, beforeDate["close"],
                                    beforeDate["close"],
                                    beforeDate["close"], beforeDate["close"]))
                            else:
                                beforeDate["open"] = data[i]["open"]
                                beforeDate["high"] = data[i]["high"]
                                beforeDate["low"] = data[i]["low"]
                                beforeDate["close"] = data[i]["close"]
                                beforeDate["volume"] = data[i]["volume"]
                                cursor.execute(sql, (
                                    instId, sysdatetime, cndatetime, interval, beforeDate["volume"],
                                    beforeDate["open"],
                                    beforeDate["high"],
                                    beforeDate["low"], beforeDate["close"]))
                        optime = nextOptime(next_optime, bar)
                    response.close()
                    db.commit()
                    print("数据插入成功，本次插入交易对：{}/{}，时间级别：{}，插入的时间：{}".format(trad1, trad2, interval,optime))
                    LocalTime = nowTime2
                except Exception as e:
                    response.close()
                    cursor.close()
                    db.close()
                    print("AA:", e,bar,trad1,trad2)
                    break
            time.sleep(1)
        print("出现了异常，跳出循环重新执行")
        conn_mysql(trad1, trad2, bar)
    except Exception as e:
        print("出现异常：", e," 异常位置：",bar,trad1,trad2)


# 计算结束时间
def calculation_endtime(optime, bar):
    dayNum = bar / 60
    endTime1 = datetime.datetime.strptime(str(optime), "%Y-%m-%d %H:%M:%S")
    endTime2 = (endTime1 + datetime.timedelta(days=dayNum)).strftime("%Y-%m-%d %H:%M:%S")
    # 判断结束时间是否小于当前时间
    nowTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if endTime2 <= nowTime:
        return endTime2
    else:
        return nowTime


def queryMysql(optime, cursor, query_Sql, beforeDate,bar):
    cursor.execute(query_Sql)
    rest = cursor.fetchone()
    if rest != None:
        optime = rest[2]
        # optime = nextOptime(optime,bar)
        beforeDate["open"] = rest[6]
        beforeDate["high"] = rest[7]
        beforeDate["low"] = rest[8]
        beforeDate["close"] = rest[9]
        beforeDate["volume"] = rest[5]
        print("捕获数据库时间戳！",optime)
    return optime


def nextOptime(next_optime, bar):
    if bar in (60, 180, 300, 900, 1800):

        minutes = barInfo[bar].replace("m", "")
        next_optime = (next_optime + datetime.timedelta(minutes=int(minutes)))
        return next_optime
    if bar in (3600, 7200, 14400, 21600, 43200):

        hours = barInfo[bar].replace("h", "")
        next_optime = (next_optime + datetime.timedelta(hours=int(hours)))
        return next_optime
    if bar == 86400:
        days = barInfo[bar].replace("d", "")
        next_optime = (next_optime + datetime.timedelta(days=int(days)))
        return next_optime



if __name__ == '__main__':
    threadpool = ThreadPoolExecutor(20)
    # threadpool.submit(conn_mysql, "BTC", "USD", 60)
    # threadpool.submit(conn_mysql, "BTC", "USD", 180)
    # threadpool.submit(conn_mysql, "BTC", "USD", 300)
    # threadpool.submit(conn_mysql, "BTC", "USD", 900)
    # threadpool.submit(conn_mysql, "BTC", "USD", 1800)
    # threadpool.submit(conn_mysql, "BTC", "USD", 3600)
    # threadpool.submit(conn_mysql, "BTC", "USD", 7200)
    # threadpool.submit(conn_mysql, "BTC", "USD", 14400)
    # threadpool.submit(conn_mysql, "BTC", "USD", 21600)
    # threadpool.submit(conn_mysql, "BTC", "USD", 43200)
    # threadpool.submit(conn_mysql, "BTC", "USD", 86400)
    #threadpool.submit(conn_mysql, "ETH", "USD", 60)
    #threadpool.submit(conn_mysql, "ETH", "USD", 180)
    threadpool.submit(conn_mysql, "ETH", "USD", 300)
    #threadpool.submit(conn_mysql, "ETH", "USD", 900)
    #threadpool.submit(conn_mysql, "ETH", "USD", 1800)
    #threadpool.submit(conn_mysql, "ETH", "USD", 3600)
    #threadpool.submit(conn_mysql, "ETH", "USD", 7200)
    #threadpool.submit(conn_mysql, "ETH", "USD", 14400)
    #threadpool.submit(conn_mysql, "ETH", "USD", 21600)
    #threadpool.submit(conn_mysql, "ETH", "USD", 43200)
   # threadpool.submit(conn_mysql, "ETH", "USD", 86400)

