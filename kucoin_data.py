import time
import asyncio
import argparse
import logging
import pandas as pd

import logging
from logging.handlers import TimedRotatingFileHandler
import gzip
import zstandard

from kucoin.client import Market as spotMarket
from kucoin.client import WsToken as spotWsToken
from kucoin.ws_client import KucoinWsClient
from kucoin.websocket.websocket import ConnectWebsocket as spotConnectWebsocket


from kucoin_futures.client import Market as futMarket
from kucoin_futures.client import WsToken as futWsToken
from kucoin_futures.ws_client import KucoinFuturesWsClient
from kucoin_futures.websocket.websocket import ConnectWebsocket as futConnectWebsocket

DATA_LOGLEVEL = 25  # INFO is 20, WARNING is 30
DATATIMEOUT = 60 # kucoin server will disconnect if they have not received the ping from the client for 60 seconds

class GzTimedRotatingFileHandler(TimedRotatingFileHandler):

    def __init__(self,gzFileName,when='h', interval=1, backupCount=0, encoding=None, delay=False, utc=False, atTime=None, useGz = False):
        self.useGz = useGz
        TimedRotatingFileHandler.__init__(self, filename=gzFileName,when=when, interval=interval, backupCount=backupCount, encoding=encoding, delay=delay, utc=utc, atTime=atTime)

    def _open(self):
        if self.useGz:
            return gzip.open(self.baseFilename, mode='at', encoding='utf-8')
        else:
            return zstandard.open(self.baseFilename, mode='at', encoding='utf-8')

def setLogger(args):
    logger = logging.getLogger()
    formatter = logging.Formatter("%(created)f;%(levelname)s;%(message)s")

    #fileHandler = TimedRotatingFileHandler("test.data",when="M", interval=1,utc=True)
    #fileHandler = GzTimedRotatingFileHandler(logfile,when="M", interval=1,utc=True)
    fileHandler = GzTimedRotatingFileHandler(args.base_filename,when="midnight", interval=1,utc=True,useGz=args.use_gz)
    if args.use_gz:
        fileHandler.suffix = "%Y%m%d%H%M.gz"
    else:
        fileHandler.suffix = "%Y%m%d%H%M.zst"
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    # add a DATA log level
    logging.addLevelName(DATA_LOGLEVEL, 'DATA')

    logger.setLevel(logging.INFO)

    return logger



class spotWsClient(KucoinWsClient):
    def __init__(self):
        KucoinWsClient.__init__(self)
        self._lastPongTime = time.time()

    async def _recv(self, msg):
        if 'data' in msg:
            await self._callback(msg)
        elif msg['type']=='pong':
            self._lastPongTime = time.time()
        else:
            print('spotWsClient',msg)

    @classmethod
    async def create(cls, loop, client, callback, private=False):
        self = spotWsClient()
        self._loop = loop
        self._client = client
        self._private = private
        self._callback = callback
        self._conn = spotConnectWebsocket(loop, self._client, self._recv, private)
        return self



class futWsClient(KucoinFuturesWsClient):
    def __init__(self):
        KucoinFuturesWsClient.__init__(self)
        self._lastPongTime = time.time()

    async def _recv(self, msg):
        if 'data' in msg:
            await self._callback(msg)
        elif msg['type']=='pong':
            self._lastPongTime = time.time()
        else:
            print('futWsClient',msg)

    @classmethod
    async def create(cls, loop, client, callback, private=False):
        self = futWsClient()
        self._loop = loop
        self._client = client
        self._private = private
        self._callback = callback
        self._conn = futConnectWebsocket(loop, self._client, self._recv, private)
        return self



def get_args():
    description = """kucoin streaming data recorder utilizing thread-safe logging module"""
    def formatter(prog): return argparse.ArgumentDefaultsHelpFormatter(prog, max_help_position=36)
    parser = argparse.ArgumentParser(description=description, formatter_class=formatter)
    parser.add_argument('--base-filename', '-f', dest="base_filename", type=str, default='kucoin.data'
                        , help='the base file name, actual file would be kucoin.data.YYYYMMDDHHMM.{zst,gz}')
    parser.add_argument('--gz', '-g', dest="use_gz", action='store_true'
                        , help='by default, each msg is compressed with zstd, if this is true, gz will be used instead')

    args, unknownargs = parser.parse_known_args()

    return args

async def deal_fut_msg(msg):
    #if msg['subject']=='match':
    #    print(msg)
    logging.log(DATA_LOGLEVEL, msg)


async def deal_spot_msg(msg):
    #print(msg)
    logging.log(DATA_LOGLEVEL, msg)


async def subscribeSpotMarket(spots):
    spotClient = spotWsToken()

    spot_wsclient = await spotWsClient.create(loop, spotClient, deal_spot_msg, private=False)
    subscribeMsg = '/market/match:'+','.join(spots)
    await spot_wsclient.subscribe(subscribeMsg)
    await  asyncio.sleep(0.5)
    # await ws_client.subscribe('/market/ticker:BTC-USDT,ETH-USDT')
    subscribeMsg = '/spotMarket/level2Depth50:'+','.join(spots)
    await spot_wsclient.subscribe(subscribeMsg)

    while True:
        if ( (time.time()-spot_wsclient._lastPongTime  > DATATIMEOUT+1 )) :
            print("spot PingPong data time out, server should disconnect already")
            try:
                print("spot try to reconnect at", time.time() )
                spot_wsclient = await spotWsClient.create(loop, spotClient, deal_spot_msg, private=False)
                subscribeMsg = '/market/match:' + ','.join(spots)
                await spot_wsclient.subscribe(subscribeMsg)
                await  asyncio.sleep(0.5)
                # await ws_client.subscribe('/market/ticker:BTC-USDT,ETH-USDT')
                subscribeMsg = '/spotMarket/level2Depth50:' + ','.join(spots)
                await spot_wsclient.subscribe(subscribeMsg)
            except Exception as e:
                print('spot',e)
        await asyncio.sleep(2, loop=loop)

async def subscribeFutMarket(futs):
    futClient = futWsToken()

    fut_wsclient = await futWsClient.create(loop, futClient, deal_fut_msg, private=False)
    subscribeMsg = '/contractMarket/execution:'+','.join(futs)
    await fut_wsclient.subscribe(subscribeMsg)
    await asyncio.sleep(0.5)
    subscribeMsg = '/contractMarket/level2Depth50:'+','.join(futs)
    await fut_wsclient.subscribe(subscribeMsg)

    while True:
        if ( (time.time()-fut_wsclient._lastPongTime > DATATIMEOUT+1)) :
            print("fut PingPong data time out, server should disconnect already")
            try:
                print("fut try to reconnect at ", time.time())
                fut_wsclient = await futWsClient.create(loop, futClient, deal_fut_msg, private=False)
                subscribeMsg = '/contractMarket/execution:' + ','.join(futs)
                await fut_wsclient.subscribe(subscribeMsg)
                await asyncio.sleep(0.5)
                subscribeMsg = '/contractMarket/level2Depth50:' + ','.join(futs)
                await fut_wsclient.subscribe(subscribeMsg)
            except Exception as e:
                print('fut',e)

        await asyncio.sleep(2, loop=loop)


async def main():
    args = get_args()
    setLogger(args)

    futs = ["XBTUSDTM","ETHUSDTM"]
    spots = ["BTC-USDT","ETH-USDT"]

    print("futures",futs)
    print("spot",spots)

    await asyncio.gather(
        subscribeFutMarket(futs),
        subscribeSpotMarket(spots)
    )

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except Exception as e:
        print("error" , e)
    finally:
        logging.shutdown()
        print('logging shutdown')

