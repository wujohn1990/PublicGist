import time
import asyncio
import argparse

import pandas as pd
import numpy as np

from kucoin_futures.client import Market as futMarket
from kucoin_futures.client import WsToken as futWsToken
from kucoin_futures.ws_client import KucoinFuturesWsClient as futWsClient


from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, curdoc
from functools import partial
from tornado.ioloop import IOLoop

doc = curdoc()

def updateTicker(new_data):
    global lastBid,lastAsk,tickerdata
    lastBid = float(new_data['bestBidPrice'])
    lastAsk = float(new_data['bestAskPrice'])
    nd = dict(dt=np.array([new_data['ts']]), bid=np.array([float(new_data['bestBidPrice'])]), ask=np.array([float(new_data['bestAskPrice'])]),
         buyprice=np.array([None]), sellprice=np.array([None]))
    tickerdata.stream(nd, rollover=100)

def updateBuy(new_data):
    global lasBid,lastAsk,tickerdata
    nd =  dict(dt=np.array([new_data['ts']]),bid=np.array([lastBid]),ask=np.array([lastAsk]),buyprice=np.array([new_data['price']]),sellprice=np.array([None]))
    tickerdata.stream(nd,rollover=100)

def updateSell(new_data):
    global lasBid,lastAsk,tickerdata
    nd =  dict(dt=np.array([new_data['ts']]),bid=np.array([lastBid]),ask=np.array([lastAsk]),buyprice=np.array([None]),sellprice=np.array([new_data['price']]))
    tickerdata.stream(nd, rollover=100)

async def deal_fut_msg(msg):
    if 'data' in msg:
        if msg['subject'] == 'tickerV2':
            doc.add_next_tick_callback(partial(updateTicker, msg['data']))
        elif msg['subject'] == 'match':
            if msg['data']['side'] == 'buy':
                doc.add_next_tick_callback(partial(updateBuy,msg['data']))
            else:
                doc.add_next_tick_callback(partial(updateSell,msg['data']))

async def main():
    futClient = futWsToken(key='', secret='', passphrase='', is_sandbox=False, url='')
    fut_wsclient = await futWsClient.create(None, futClient, deal_fut_msg, private=False)
    await fut_wsclient.subscribe('/contractMarket/tickerV2:XBTUSDTM')
    await fut_wsclient.subscribe('/contractMarket/execution:XBTUSDTM')



try:
    tickerdata = ColumnDataSource(
        data=dict(dt=np.array([]), bid=np.array([]), ask=np.array([]), buyprice=np.array([]),
                  sellprice=np.array([])))

    lastBid = None
    lastAsk = None
    plot = figure(height=800, width=1200)
    plot.step(x='dt', y='bid', color='blue', source=tickerdata)
    plot.step(x='dt', y='ask', color='orange', source=tickerdata)
    plot.scatter(x='dt', y='buyprice', marker='triangle', color='green', size=20, source=tickerdata)
    plot.scatter(x='dt', y='sellprice', marker='inverted_triangle', color='red', size=20, source=tickerdata)
    doc.add_root(plot)
    IOLoop.current().spawn_callback(main)
except Exception as e:
    print("error" , e)
finally:
    pass





