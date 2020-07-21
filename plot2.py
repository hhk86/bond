import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import sys
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
from jinja2 import Template
from dateutil.parser import parse as dateparse
import os


class TsTickData(object):

    def __enter__(self):
        if ts.Logined() is False:
            print('天软未登陆或客户端未打开，将执行登陆操作')
            self.__tsLogin()
            return self

    def __tsLogin(self):
        ts.ConnectServer("tsl.tinysoft.com.cn", 443)
        dl = ts.LoginServer("fzzqjyb", "fz123456")
        print('天软登陆成功')

    def __exit__(self, *arg):
        ts.Disconnect()
        print('天软连接断开')

    def ticks(self, code, start_date, end_date):
        ts_template = Template('''begT:= StrToDate('{{start_date}}');
                                  endT:= StrToDate('{{end_date}}');
                                  setsysparam(pn_cycle(),cy_1s());
                                  setsysparam(pn_rate(),0);
                                  setsysparam(pn_RateDay(),rd_lastday);
                                  r:= select  ["StockID"] as 'ticker', datetimetostr(["date"]) as "time", ["price"]
                                      from markettable datekey begT to endT of "{{code}}" end;
                                  return r;''')
        ts_sql = ts_template.render(start_date=dateparse(start_date).strftime('%Y-%m-%d'),
                                    end_date=dateparse(end_date).strftime('%Y-%m-%d'),
                                    code=code)

        fail, data, _ = ts.RemoteExecute(ts_sql, {})

        def gbk_decode(strlike):
            if isinstance(strlike, (str, bytes)):
                strlike = strlike.decode('gbk')
            return strlike

        def bytes_to_unicode(record):
            return dict(map(lambda s: (gbk_decode(s[0]), gbk_decode(s[1])), record.items()))

        if not fail:
            unicode_data = list(map(bytes_to_unicode, data))
            return pd.DataFrame(unicode_data).set_index(['time', 'ticker'])
        else:
            raise Exception("Error when execute tsl")

def qty2size(n):
    if n <= 200:
        return 1
    elif n <= 500:
        return 1.5
    elif n <= 1000:
        return 2
    elif n <= 2000:
        return 3
    elif n <= 3000:
        return 4
    elif n <= 4000:
        return 5
    else:
        return 6


if __name__ == "__main__":

    df = pd.read_excel("511260.xlsx", encoding="gbk", skiprows=4)
    df = df[["成交时间", "买卖方向", "成交价格", "成交数量"]]
    df = df.iloc[:-1,:]
    df["date"] = df["成交时间"].apply(lambda s: str(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").date()))
    df["time"] = df["成交时间"].apply(lambda s: str(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").time()))
    # df.to_csv("debug.csv", encoding="gbk")
    date_ls = sorted(list(set(df["date"].to_list())))
    date_ls = [s[:4] + s[5:7] + s[8:] for s in date_ls]
    date = date_ls[-1]
    date_next = "20200721"
    print(date, date_next)
    df2 = df[df["date"] == date[:4] + '-' + date[4:6] + '-' + date[6:]]





    with TsTickData() as obj:
        data1 = obj.ticks(code="511260.SH", start_date=date, end_date=date_next)
    data1["index"] = data1.index
    data1["time"] = data1["index"].apply(lambda tu: tu[0][-8:])
    data1 = data1[(data1["time"] <= "11:30:00") | (data1["time"] >= "13:00:00")]
    data1["time_offset"] = list(range(data1.shape[0]))
    pd.set_option("display.max_columns", None)
    data1 = data1.set_index(keys="time_offset", drop=False)
    data1 = data1.drop("index", axis=1)

    print(df2)
    print(data1)

    df2 = pd.merge(df2, data1, on="time")
    df2['color'] = df2['买卖方向'].apply(lambda s: "red" if s == "买入" else "green")
    df2['markersize'] = df2['成交数量'].apply(qty2size)
    df2["price"] = df["成交价格"]
    df2 = df2[["time_offset", "price", "markersize", "color"]]
    print(df2)

    sys.exit()





    date = "20191218"  #今日日期
    end_date = "20191219" #迫于天软查询语句，需要设置一个未来日期
    df = pd.read_excel("当日委托20191218161549.xlsx", encoding="gbk")
    df.sort_values(by="时间", inplace=True)
    df["ticker"] = df["代码/名称"].apply(lambda s: "SH" + s[:6] if s.startswith('6') else "SZ" + s[:6])
    df["name"] = df["代码/名称"].apply(lambda s: s[7:])
    df.columns = ["time", "ticker/name", "direction", "quantity", "price", "status", "ticker", "name"]
    df = df[["time", "ticker", "name", "price", "quantity", "direction", "status"]]
    try:
        df["time"] = df["time"].apply(lambda s: str(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").time()))
        print("This is historical order format.")
    except ValueError:
        df["time"] = df["time"].apply(lambda s: str(dt.datetime.strptime(s, "%H:%M:%S").time()))
        print("This is today's order format.")
    ticker_set = set(sorted(df["ticker"].tolist()))
    i = 0
    try:
        os.makedirs("pictures_" + date +'/')
    except FileExistsError:
        pass
    for ticker in ticker_set:
        with TsTickData() as obj:
            data = obj.ticks(code=ticker, start_date=date, end_date=end_date)
        data["index"] = data.index
        data["time"] = data["index"].apply(lambda tu: tu[0][-8:])
        data = data[(data["time"] <= "11:30:00") | (data["time"] >= "13:00:00")]
        data["time_offset"] = list(range(data.shape[0]))
        plt.figure(figsize=(20, 10))
        plt.plot(data["time_offset"], data["price"], color="gray", alpha=0.5)
        sub_df = df[df["ticker"] == ticker]
        sub_df["pct"] = round(sub_df["quantity"] / sub_df["quantity"].sum() * 100).astype(int)
        for key, record in sub_df.iterrows():
            if (record["time"] >= "11:30:00" and record["time"] <= "13:00:00") or record["time"] < "09:30:00" or record["time"] > "15:00:00":
                print("Entrust order at illegal time!!!")
                continue
            if record["direction"] == "买入" and record["status"] == "已成":
                marker = 'or'
            elif record["direction"] == "卖出" and record["status"] == "已成":
                marker = 'sg'
            elif record["direction"] == "买入" and record["status"] == "已撤":
                marker = '*r'
            elif record["direction"] == "卖出" and record["status"] == "已撤":
                marker = 'xg'
            time_offset = data[data["time"] == record["time"]]["time_offset"].squeeze()
            try:
                plt.plot([time_offset,], [record["price"],], marker, markersize=6)
            except ZeroDivisionError:
                print("integer division or modulo by zero")
                print(time_offset)
                print(record["price"])
            plt.text(time_offset, record["price"] + 0.01, str(record["pct"]) +'%')
        xticks = list(range(14401))[::1800]
        xticklabels = ["9:30", "10:00", "10:30", "11:00", "11:30/13:00", "13:30", "14:00", "14:30", "15:00"]
        plt.xticks(xticks, xticklabels)
        plt.title(ticker + " | " + date, fontsize=25)
        plt.savefig("pictures_" + date +'/' + ticker + '_' + date + '.png')
        plt.close()
        i += 1
        print("Process " + str(i) + " stocks")