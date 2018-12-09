# _*_ encoding:utf-8 _*_
__author__ = 'jammy'
__wechat__ = 'w1184252762'
__date__ = '2018/12/5 20:20'


import requests
import ccxt
import pandas as pd
import time
import pymongo


# 使用 mongo 数据库存储数据
client = pymongo.MongoClient(host='localhost', port=27017)
binance_db = client['binance_db']
binance_db_data = binance_db['binance_db_data']

# 设置这个使得经pandas处理的数据全部显示
pd.set_option('expand_frame_repr', False)

# 通过ccxt初始化交易所，使用私有 API，传入用户api_key信息
binance_exchange = ccxt.binance({
    'apiKey': 'ZDQgeMx5RuAukGhmHMnCqHxLvHRVf83x11REMVNUpDJhapqei7OIWPl9jziJan72',
    'secret': '05d7E54OkeWqwidvlCKFuOTBkgiujcdEYOAvOdlajUknGvWSHtRi70P2z0QdacfF',
    'timeout': 15000,
    'enableRateLimit': True
})
print('初始化私有API交易所，完成！')
# 选择两个交易市场 A B
market_a, market_b = 'BTC', 'ETH'


# 查询账户币信息，返回某个币 free 可用数量，或None
def account_info(coin='BTC'):
    print('开始查询账户{}信息，'.format(coin))
    try:
        total = binance_exchange.fetch_balance()['total'][coin]
        free = binance_exchange.fetch_balance()['free'][coin]
        used = binance_exchange.fetch_balance()['used'][coin]
        print('{}币：一共{}，可用{}，冻结{}。'.format(coin, total, free, used))
        return free
    except Exception as e1:
        print('您没有{}币，或者查询出错。见错误信息：{}'.format(coin, e1))
        return None


# 查询未成交的订单(这里以 ETH/BTC 为例)，返回 0 没有订单，或返 1 有订单
def query_open(symbol='ETH/BTC'):
    print('开始查询未成交{}订单信息，'.format(symbol))
    try:
        if binance_exchange.has['fetchOpenOrders']:
            open_orders = binance_exchange.fetch_open_orders(symbol)
            if len(open_orders) < 1:
                print('没有未成交的{}交易对的订单。'.format(symbol))
                return 0  # 表示没有订单
            else:
                print('有{}个未成交的{}交易对的订单。'.format(len(open_orders), symbol))
                return 1  # 表示有订单
    except Exception as e2:
        print('查询失败，错误：{}'.format(e2))
        return None


# 取消订单
def cancelll(symbol='ETH/BTC'):
    print('开始取消{}订单，'.format(symbol))
    # 先查询到哪些未成交的订单
    if binance_exchange.has['fetchOpenOrders']:
        open_orders = binance_exchange.fetch_open_orders(symbol)
        # 然后取消订单
        if len(open_orders) > 0 and binance_exchange.has['cancelOrder']:
            # 可能某个交易对有多个订单
            print('{}交易对，一共{}个未成交订单。'.format(symbol, len(open_orders)))
            for open_order in open_orders:
                # 获取未成交订单的id
                order_id = open_order['info']['orderId']
                # 根据订单id，以及交易对名称，使用 cancel_order() 取消该交易对某个id的订单
                binance_exchange.cancel_order(order_id, symbol)
                print('已取消{}交易对中id为{}的订单。'.format(symbol, order_id))
        else:
            print('您没有{}相关的订单。'.format(symbol))


# 创建订单示例方法
def xiadan(symbol='BTC/USDT', side='buy', price=2000, amount=0.01):
    try:
        if binance_exchange.has['createLimitOrder']:
            # 参数：symbol》交易对，side》买buy还是卖sell，type》使用市价base还是自定义价格limit，price》自定义价格，amount》买卖币数量
            binance_exchange.create_order(symbol=symbol, side=side, type='limit', price=price, amount=amount)
            return 1
    except Exception as e6:
        print('创建 ETH/BTC 订单失败，错误：{}'.format(e6))
        return 0


# 实际下单分解步骤一：用拥有的 BTC 去购买 ETH , 价格设为 p1。返回 0 失败，或 1 成功
def btc_buy_eth(p1, amount=1):
    symbol = market_b + '/' + market_a
    try:
        binance_exchange.create_order(symbol=symbol, side='buy', type='limit', price=p1, amount=amount)
        print('创建订单第 1 步完成：ETH/BTC。')
        return 1
    except Exception as e3:
        print('创建 ETH/BTC 订单失败，错误：{}'.format(e3))
        return 0


# 实际下单分解步骤二：用拥有的 ETH 去购买 C币，价格设置为 p2。返回 0 失败，或 1 成功
def eth_buy_c(market_c, p2, amount):
    symbol = market_c + '/' + market_b
    try:
        binance_exchange.create_order(symbol=symbol, side='buy', type='limit', price=p2, amount=amount)
        print('创建订单第 2 步完成：{}。'.format(symbol))
        return 1
    except Exception as e4:
        print('创建 {} 订单失败，错误：{}'.format(symbol, e4))
        return 0


# 实际下单分解步骤三：用拥有的 C币 去卖，换成BTC，价格设置为 p3。返回 0 失败，或 1 成功
def c_sell_btc(market_c, p3, amount):
    symbol = market_c + '/' + market_a
    try:
        binance_exchange.create_order(symbol=symbol, side='sell', type='limit', price=p3, amount=amount)
        print('创建订单第 3 步完成：{}。'.format(symbol))
        return 1
    except Exception as e5:
        print('创建 {} 订单失败，错误：{}'.format(symbol, e5))
        return 0


# 通过binance官方API获取所有市场行情，返回所有可交易的市场交易对
def get_binance_markets():
    # 返回 例如：['ETH/BTC', 'HOT/BTC', 'HOT/ETH']
    url = 'https://api.binance.com/api/v1/exchangeInfo'
    rep = requests.get(url)
    rep_js = rep.json()
    symbols = rep_js['symbols']
    binance_markets_list = []
    for symbol in symbols:
        base = symbol['baseAsset']
        quote = symbol['quoteAsset']
        status = symbol['status'] == 'TRADING'
        if status:
            exchange = base + '/' + quote
            binance_markets_list.append(exchange)
    return binance_markets_list


# 获取binance和ccxt_binance共同的交易对中，market_a和market_b共有的基础货币，返回 common_base_list
def get_common_base_list():
    # 返回 例如：['HOT', 'ETH', 'BTC', 'EOS']
    # 通过 binance 官方API获取市场交易对
    binance_markets = get_binance_markets()
    # 通过 ccxt 加载所有市场交易对
    ccxt_markets = list(binance_exchange.load_markets().keys())
    # 取两个方式获取的共同存在的市场对
    common_symbol_list = list(set(binance_markets).intersection(set(ccxt_markets)))
    # print('共{}个相同交易对'.format(len(common_symbol_list)))
    # 存放至 pandas 的 DataFrame 中
    symbols_df = pd.DataFrame(data=common_symbol_list, columns=['symbol'])
    # 分割字符串得到 基础货币base/计价货币quote
    base_quote_df = symbols_df['symbol'].str.split(pat='/', expand=True)
    base_quote_df.columns = ['base', 'quote']
    # 过滤得到以 A,B 计价的基础货币
    base_a_list = base_quote_df[base_quote_df['quote'] == market_a]['base'].values.tolist()
    base_b_list = base_quote_df[base_quote_df['quote'] == market_b]['base'].values.tolist()
    # 获取相同的基础货币列表
    common_base_list = list(set(base_a_list).intersection(set(base_b_list)))
    # print('{}和{}，共有{}个相同的基础货币'.format(market_a, market_b, len(common_base_list)))
    return common_base_list


# 根据common_base_list中一个 commom_base，然后组合成3组交易对，然后返回交易对的列表 symbol_list
def get_symbol_list(common_base):
    # 传入C币，例如：HOT，返回 [ETH/BTC, HOT/ETH, HOT/BTC]
    market_c = common_base
    market_a2b_symbol = '{}/{}'.format(market_b, market_a)
    market_b2c_symbol = '{}/{}'.format(market_c, market_b)
    market_c2a_symbol = '{}/{}'.format(market_c, market_a)
    symbol_list = [market_a2b_symbol, market_b2c_symbol, market_c2a_symbol]
    return symbol_list


# 获取某个 symbol 的交易委托信息，返回 price_lh，包括最高买价bid，最低卖价ask
def get_price_list(symbol):
    # 传入交易对，例如：ETH/BTC，返回 最高买价bid和最低卖价ask
    orderbook = binance_exchange.fetch_order_book(symbol)
    # 最高买价（买家的出价，对于想要卖的人来说，越高越好）
    bid = orderbook['bids'][0][0] if len(orderbook['bids']) > 0 else None
    # 最低卖价（卖家的出价，对于想要买的人来说，越低越好）
    ask = orderbook['asks'][0][0] if len(orderbook['asks']) > 0 else None
    # print('最高买价：{}, 最低卖价：{}'.format(bid, ask))
    price_lh = [bid, ask]
    return price_lh


# 根据3个交易对的委托买卖价格列表，计算利润率，返回价格信息可以执行套利，如果返回 None 就不能套利
def profit_calculated(price_list, floorprofit=6, skyprofit=100):
    # 传入 价格列表（p1/p2/p3各自的最高买价和最低卖价）和利润下限值，返回 利润值/p1买价/p2买价/p3卖价
    p1_l, p1_h, p2_l, p2_h, p3_l, p3_h = price_list
    # 正常来说，最高买价低于最低卖价，l 价格低于 h，选择价格
    p1 = p1_l
    p2 = p2_h
    p3 = p3_h
    if 0 not in price_list and None not in price_list:
        # profit_highest = (p3_h / (p1_l * p2_l) - 1) * 1000
        profit_m = (p3 / (p1 * p2) - 1) * 1000
        if floorprofit <= profit_m <= skyprofit:
            # price_info = [profit_highest, p1_l, p2_l, p3_h]
            price_info = [profit_m, p1, p2, p3]
            return price_info
        else:
            return None  # 不能套利
    else:
        return None  # 不能套利


# 主函数第 1 部分，获取可套利组合的信息 market_c/p1_low/p2_low/p3_high
def main_first():
    # 返回可套利的c币信息和利润率以及价格信息 例如：['HOT', 64.32490859349626, 0.025199, 5.22e-06, 1.4e-07]
    # 优先处理的基础币
    priority_base = ['BQX', 'DGD', 'PHX', 'STORJ', 'DLT', 'CDT', 'NAV', ]
    # 一定不用的基础币，也就是经分析判断，可获利但是可能存在很多量化程序都在追逐的币，交易可能性基本为0.
    unused_base = ['SC', 'NPXS', 'DENT', 'HOT', 'STORM', 'POE', 'MFT', 'VET', 'BCD', 'FUN', 'TNB', 'NCASH', 'MTL', 'KEY', 'LEND', 'CND', 'SKY', 'IOTX', 'GXS', 'RCN', 'IOST', 'TNT', 'XVG']
    # 然后将优先处理 priority_base，再处理去除掉 unused_base  之后的共同基础币 get_common_base_list()
    common_base_list = priority_base + list( set(get_common_base_list()) - set(unused_base) )
    for common_base in common_base_list:
        symbol_list = get_symbol_list(common_base)
        price_list = []
        for symbol in symbol_list:
            price_lh = get_price_list(symbol)
            price_list.extend(price_lh)
            time.sleep(binance_exchange.rateLimit / 1000)
        price_info = profit_calculated(price_list)
        if price_info is None:
            print('{:<7s} pass。'.format(common_base))
        else:
            print('{:<7s} get it！'.format(common_base))
            print('rate(1/1000)/p1/p2/p3分别是{}'.format(price_info))
            price_info.insert(0, common_base)
            data = {
                'c币': common_base,
                '利润(1/1000)': price_info[1],
                'p1价格': price_info[2],
                'p2价格': price_info[3],
                'p3价格': price_info[4],
            }
            binance_db_data.insert_one(data)
            # return price_info ########################################################################################
    return None


# 主函数第 2 部分，完成订单操作
def main_second(information):
    market_c, profit_rate, p1, p2, p3 = information
    # 第一步
    #  - 查询账户中有多少BTC，account_info(coin='BTC') 返回可用BTC数量，使用 99.9998% 的BTC去交易,
    #  - 下单：BTC -> ETH，btc_buy_eth(p1, amount=1)，
    #  - 确认这一步完成（使用query_open(orders='ETH/BTC') 返回1表示订单还在，返回0表示交易完成）后才进行下一步。
    btc_count_old = account_info('BTC')
    while btc_count_old is None:
        btc_count_old = account_info('BTC')
    buy_amount_step1 = (btc_count_old * 0.999998) / p1
    fail_success_step1 = btc_buy_eth(p1, amount=buy_amount_step1)
    while fail_success_step1 == 0:
        fail_success_step1 = btc_buy_eth(p1, amount=buy_amount_step1)
    s1 = query_open(symbol='ETH/BTC')
    while s1 is None or s1 == 1:
        s1 = query_open(symbol='ETH/BTC')
    if s1 == 0:
        # 第二步
        #  - 查询账户中有多少ETH，account_info(coin='ETH') 返回可用ETH数量
        #  - 下单：ETH ->  C，eth_buy_c(market_c, p2, amount)
        #  - 确认这一步完成（使用query_open(orders='market_c/ETH') 返回1表示订单还在，返回0表示交易完成）后才进行下一步。
        eth_count = account_info('ETH')
        while eth_count is None:
            eth_count = account_info('ETH')
        buy_amount_step2 = (eth_count * 0.999998) / p2
        fail_success_step2 = eth_buy_c(market_c, p2, amount=buy_amount_step2)
        while fail_success_step2 == 0:
            fail_success_step2 = eth_buy_c(market_c, p2, amount=buy_amount_step2)
        orders_2 = market_c + '/ETH'
        s2 = query_open(symbol=orders_2)
        while s2 is None or s2 == 1:
            s2 = query_open(symbol='orders_2')
        if s2 == 0:
            # 第三步
            #  - 查询账户中有多少C币， account_info(coin=market_c) 返回可用market_c数量
            #  - 下单：C ->  BTC，c_sell_btc(market_c, p3, amount)
            #  - 确认这一步完成（使用query_open(orders='market_c/BTC') 返回1表示订单还在，返回0表示交易完成）后才进行下一步。
            c_count = account_info(market_c)
            while c_count is None:
                c_count = account_info(market_c)
            fail_success_step3 = c_sell_btc(market_c, p3, amount=c_count)
            while fail_success_step3 == 0:
                fail_success_step3 = c_sell_btc(market_c, p3, amount=c_count)
            orders_3 = market_c + '/BTC'
            s3 = query_open(orders_3)
            while s3 is None or s3 == 1:
                s3 = query_open(orders_3)
            if s3 == 0:
                btc_count_new = account_info('BTC')
                while btc_count_new is None:
                    btc_count_new = account_info('BTC')
                btc_more = btc_count_new - btc_count_old
                true_profit_rate = btc_more / btc_count_old
                print('套利完成，当前拥有BTC{}个，增加{}个，利润率为{}。'.format(btc_count_new, btc_more, true_profit_rate))


if __name__ == "__main__":
    n = 1
    while True:
        start_time = time.time()
        try:
            info = main_first()
        except Exception as e:
            print('访问有问题！')
            print(e)
        else:
            if info is not None:
                # print('套利所需信息{}传给下一步套利！'.format(info))
                print('执行套利操作：')
                # main_second(info)
            else:
                print('没有可套利组合。')
            end_time = time.time()
            used_time = end_time - start_time
            print('第{}次操作完成，所花时间：{}秒。'.format(n, used_time))
            n += 1
        finally:
            print('*' * 50)
            time.sleep(5)
