import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import datetime as dt
import sys
sys.path.append("D:\\Program Files\\Tinysoft\\Analyse.NET")
import TSLPy3 as ts
from jinja2 import Template
from dateutil.parser import parse as dateparse
matplotlib.rcParams['font.family'] = ' STSong'


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

def qty2size(n, type):
    if type == "spot":
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
    if type == "future":
        if n == 1:
            return 4
        elif n == 2:
            return 6
        elif n <= 4:
            return 8
        else:
            return 12

def get_full_data(date, date_next, ticker):
    with TsTickData() as obj:
        data = obj.ticks(code=ticker, start_date=date, end_date=date_next)
    data["index"] = data.index
    data["time"] = data["index"].apply(lambda tu: tu[0][-8:])
    data = data[((data["time"] <= "11:30:00") | (data["time"] >= "13:00:00")) & (data["time"] <= "15:00:00") & (data["time"] >= "09:30:01")]
    data["time_offset"] = list(range(data.shape[0]))
    data = data.set_index(keys="time_offset", drop=False)
    data = data.drop("index", axis=1)
    return data

def process_transaction_data(transaction, full, type):
    data = pd.merge(transaction, full, on="time")
    color1 = "red" if type == "spot" else "blue"
    color2 = "green" if type == "spot" else "darkviolet"
    data['color'] = data['买卖方向'].apply(lambda s: color1 if s == "买入" else color2)
    data['markersize'] = data['成交数量'].apply(qty2size, args=(type,))
    data["price"] = data["成交价格"]
    data = data[["time_offset", "price", "markersize", "color"]]
    return data




def get_chart(spot, future, spot_ticker, future_ticker):
    date_ls = sorted(list(set(spot["date"].to_list())))
    date_ls = [s[:4] + s[5:7] + s[8:] for s in date_ls]
    date = date_ls[-1]
    date_next = "20200721"
    print(date, date_next)
    spot_transaction_raw = spot[spot["date"] == date[:4] + '-' + date[4:6] + '-' + date[6:]]
    spot_full = get_full_data(date, date_next, spot_ticker)
    spot_transaction = process_transaction_data(spot_transaction_raw, spot_full, type="spot")


    future_transaction_raw = future[future["date"] == date[:4] + '-' + date[4:6] + '-' + date[6:]]
    future_full = get_full_data(date, date_next, future_ticker)
    future_transaction = process_transaction_data(future_transaction_raw, future_full, type="future")
    basis_full = pd.merge(spot_full, future_full, left_index=True, right_index=True)
    basis_full["basis"] = basis_full["price_y"] - basis_full["price_x"]
    basis_full = basis_full[["basis", "time_offset_x", "time_x"]]
    basis_full.columns = ["basis", "time_offset", "time"]
    print(basis_full)
    basis_transaction = pd.merge(future_transaction_raw, basis_full, on="time")
    basis_transaction["color"] = basis_transaction["买卖方向"].apply(lambda s: "green" if s == "买入" else "red")
    basis_transaction["markersize"] = basis_transaction["成交数量"].apply(qty2size, args=("future",))
    basis_transaction["action"] = basis_transaction["买卖方向"].apply(lambda s: "减" if s == "买入" else "加")

    fig = plt.figure(figsize=(18,8))
    ax1 = fig.add_subplot(211)
    ax1.plot(spot_full["time_offset"], spot_full["price"], color="lightgrey")
    ax2 = ax1.twinx()
    ax2.plot(future_full["time_offset"], future_full["price"], color="moccasin")
    for key, value in spot_transaction.iterrows():
        ax1.plot([value["time_offset"],], [value["price"],], marker='o', markersize=value["markersize"], color=value["color"])
    for key, value in future_transaction.iterrows():
        ax2.plot([value["time_offset"],], [value["price"],], marker='o', markersize=value["markersize"], color=value["color"], linewidth=0.5)

    ax3 = fig.add_subplot(212)
    ax3.plot(basis_full["time_offset"], basis_full["basis"], color="lightgrey")
    for key, value in basis_transaction.iterrows():
        # ax3.plot([value["time_offset"],], [value["basis"],], marker='o', markersize=value["markersize"], color=value["color"], linewidth=0.5)
        ax3.text(value["time_offset"], value["basis"], value["action"], color=value["color"])
    plt.show()


if __name__ == "__main__":


    spot = pd.read_excel("511260.xlsx", encoding="gbk", skiprows=4)
    spot = spot[["成交时间", "买卖方向", "成交价格", "成交数量"]]
    spot = spot.iloc[:-1,:]
    spot["date"] = spot["成交时间"].apply(lambda s: str(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").date()))
    spot["time"] = spot["成交时间"].apply(lambda s: str(dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").time()))


    future = pd.read_excel("T2009.xls", encoding="gbk")
    future = future[["日期", "成交时间", "委托方向", "成交价格", "成交数量"]]
    future = future.iloc[:-1, :]
    future["委托方向"] = future["委托方向"].apply(lambda s: s[:2])
    future.columns = ["date", "time", "买卖方向", "成交价格", "成交数量"]
    pd.set_option("display.max_columns", None)

    get_chart(spot, future, "511260.SH", "T2009")



