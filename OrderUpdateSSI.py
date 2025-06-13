import json
from threading import Thread
from time import sleep, time

from Console.Helper.Defaults import *
from Console.Helper.Redis import Redis
from Console.Helper.Telegram import Tele
from Console.Lib.SSI.SSI import SSIClient, SSIOrder
from Console.Lib.SSI.SSI_socket import SSIStream
from Console.Helper.Timer import getSession

class OrderUpdateSSI():

    def __init__(self, account_id) -> None:
        self.accountId = account_id
        self.count = 0
        self.lastUpdateTime = int(time()*1000)
  
    def update(self):

        account_id = self.accountId
       
        self.lastNotifyIDRediskey = f'stock_bot_last_notifyid_{self.accountId}_{account_id}'
        self.redisOnChangePubsub = f"stock_bot_order_change_pubsub"
        self.redisTimeout = 8 * 60 * 60
        self.redisData = None
        self.requestData = None
        
        self.chatBot = Tele.TELE_BOT_DEFAULT
        self.errorGr = Tele.TELE_GROUP_DEFAULT
        self.noticeGr = Tele.TELE_GROUP_DEFAULT
        
        self.client:SSIClient = SSIClient(account_id)
        self.client.getRedisAuth()

        if(not self.client.authorized):
            self.client.authorize()
        
        self.account = self.client.account
        self.chatBot = Tele.getBot(self.account.account_tele_bot)
        self.errorGr = Tele.getGroup(self.account.account_tele_gr_error)
        self.noticeGr = Tele.getGroup(self.account.account_tele_gr_notice)

        self.redis = Redis()
        if(not self.client.authorized): raise Exception('Can not login to account')
        last_id = self.redis.get(self.lastNotifyIDRediskey)
        last_id = 0 if last_id is None else last_id
        last_id = -1
        self.loadOrderInday()
        self.socket = SSIStream(account_id, handle_message= self.onNewMessage, handle_error=self.onErrorMessage, handle_close=self.onClose, handle_exception=self.onException, notifyID=str(last_id))
        self.socket.start()
        monitorTimeoutThread = Thread(target=self.monitorTimeoutTarget, daemon=True)
        monitorTimeoutThread.start()
        
        Tele.send(f'{Tele.TELE_ICON_TAKEPROFIT} Socket {self.account.account_name} Connected', self.errorGr, self.chatBot)
        self.socket.keep_alive()

    def onNewMessage(self, message):
        try:
            # self.count +=1
            # print(message)
            # {"type":"orderEvent","data":{"orderID":"86769316","notifyID":67869,"data":null,"instrumentID":"SSI","uniqueID":"35207020","buySell":"B","orderType":"LO","ipAddress":"10.232.81.31","price":31000.0,"prefix":"dgd","quantity":100,"marketID":"VN","origOrderId":"","account":"0658621","cancelQty":0,"osQty":100,"filledQty":0,"avgPrice":0.0,"channel":"IW","inputTime":"1704335090000","modifiedTime":"1704335091000","isForceSell":"F","isShortSell":"F","orderStatus":"RS","rejectReason":"0","origRequestID":"35207020","stopOrder":false,"stopPrice":0.0,"stopType":"","stopStep":0.0,"profitPrice":0.0,"modifiable":true}}
            # {"type":"orderEvent","data":{"orderID":"86769316","notifyID":67879,"data":null,"instrumentID":"SSI","uniqueID":"35207020","buySell":"B","orderType":"LO","ipAddress":"10.232.81.31","price":31000.0,"prefix":"dgd","quantity":100,"marketID":"VN","origOrderId":"","account":"0658621","cancelQty":0,"osQty":100,"filledQty":0,"avgPrice":0.0,"channel":"IW","inputTime":"1704335090000","modifiedTime":"1704335091000","isForceSell":"F","isShortSell":"F","orderStatus":"QU","rejectReason":"0","origRequestID":"35207020","stopOrder":false,"stopPrice":0.0,"stopType":"","stopStep":0.0,"profitPrice":0.0,"modifiable":true}}
            log(message)
            message:dict = json.loads(message)
            if(message['type'] == 'orderEvent'):
                # self.count +=1
                # print(self.count)
                data:dict = message['data']
                if(type(data) is dict and 'orderID' in data):
                    orderID = data['orderID']
                    accountNumber = data['account']
                    if(accountNumber == self.account.account_number):
                        order = SSIOrder.parse(data)
                        perOrderRedisKey = f"stock_bot_order_{self.accountId}_{orderID}"    
                        orderData = self.redis.get(perOrderRedisKey)
                        if(orderData is None):
                            orderData = {}
                            orderData['accountId'] = self.accountId
                            orderData['symbol'] = order.instrumentID
                            orderData['accountNumber'] = accountNumber
                            orderData['sellBuyType'] = order.buySell
                            orderData['orderQuantity'] = order.quantity
                            orderData['orderPrice'] = order.price
                            orderData['matchedQuantity'] = order.filledQty
                            orderData['matchedPrice'] = order.avgPrice
                            orderData['unmatchedQuantity'] = order.quantity - order.filledQty
                            orderData['orderStatus'] = order.orderStatus
                            orderData['matchedValue'] = order.filledQty * order.avgPrice
                            orderData['orderNumber'] = order.orderID
                            orderData['uniqueID'] = order.uniqueID
                            orderData['orderTime'] = order.inputTime
                            orderData['orderType'] = order.orderType
                        else:
                            orderData = json.loads(orderData)
                            orderData['accountId'] = self.accountId
                            orderData['orderQuantity'] = order.quantity
                            orderData['orderPrice'] = order.price
                            orderData['matchedQuantity'] = order.filledQty
                            orderData['matchedPrice'] = order.avgPrice
                            orderData['unmatchedQuantity'] = order.quantity - order.filledQty
                            orderData['orderStatus'] = order.orderStatus
                            orderData['matchedValue'] = order.filledQty * order.avgPrice
                        
                        
                        if(order.avgPrice is not None and order.avgPrice > 0):
                            orderData['lastPrice'] = order.avgPrice
                            if(orderData.get('firstPrice', None) is None): orderData['firstPrice'] = order.avgPrice

                        #Update to redis key. prepair for per redis key each order
                        redisData = json.dumps(orderData)
                        log(f"Push to redis {perOrderRedisKey} {redisData}")
                        self.redis.set(perOrderRedisKey, redisData, self.redisTimeout)
                        self.redis.r.publish(self.redisOnChangePubsub, redisData)
                    
                else:
                    print(data)
                    
            elif message['type'] == 'orderError':
                data  = message.get('data', {})
                Tele.send(f"{Tele.TELE_ICON_ERROR} Socket {self.account.account_name}: {data.get('message')}", self.errorGr, self.chatBot)
            
            if 'data' in message:
                data:dict = message['data']
                if 'uniqueID' in data:
                    uniqueId = data['uniqueID']
                    rqData = json.dumps(data)
                    perRequestRedisKey = f"stock_bot_request_{self.accountId}_{uniqueId}"    
                    log(f"pust request id {perRequestRedisKey} {rqData}")
                    self.redis.set(perRequestRedisKey, rqData, self.redisTimeout)
                if 'notifyID' in data:
                    self.redis.set(self.lastNotifyIDRediskey, data['notifyID'], self.redisTimeout)
    
        except Exception as e:
            err = exceptionInfo(e)
            Tele.send(f"{Tele.TELE_ICON_WARNING} Socket {self.account.account_name} {err['summary']}", self.errorGr, self.chatBot)
            
    def onErrorMessage(self, *args):
        self.socket.restart()

    def onClose(self, *args):
        log("SSI Stream closed", args)
        Tele.send(f"{Tele.TELE_ICON_WARNING} Socket {self.account.account_name} closed", self.errorGr, self.chatBot)
    
    def onException(self, *args):
        log("SSI Exception với args:", args)
        Tele.send(f"{Tele.TELE_ICON_WARNING} Socket {self.account.account_name} exception", self.errorGr, self.chatBot)

    def monitorTimeoutTarget(self):
        while True:
            try:
                session = getSession()
                if(session == 'TIMEOUT' and self.socket.is_connected):
                    sleep(300)
                    self.socket.stop()
                    print(f'{Tele.TELE_ICON_TAKEPROFIT} Socket {self.account.account_name} has been paused because of the Working Timeout')
                    Tele.send(f'{Tele.TELE_ICON_TAKEPROFIT} Socket {self.account.account_name} has been paused because of the Working Timeout', self.errorGr, self.chatBot)
                if(session != 'TIMEOUT' and not self.socket.is_connected):
                    self.socket.restart()
                    print(f'{Tele.TELE_ICON_TAKEPROFIT} Socket {self.account.account_name} resume')
                    Tele.send(f'{Tele.TELE_ICON_TAKEPROFIT} Socket {self.account.account_name} resume', self.errorGr, self.chatBot)
            finally:
                sleep(30)
    def loadOrderInday(self):
        client = SSIClient(self.account.account_id)
        res = client.get_order_book()
        if not res['result']: return
        ssiOrders: list(SSIOrder) = res['data']
        for orderID in ssiOrders:
            order:SSIOrder = ssiOrders[orderID]
            # orderID = order.orderID
            perOrderRedisKey = f"stock_bot_order_{self.account.account_id}_{orderID}"    
            orderData = self.redis.get(perOrderRedisKey)
            if(orderData is None):
                orderData = {}
                orderData['symbol'] = order.instrumentID
                orderData['accountNumber'] = self.account.account_number
                orderData['sellBuyType'] = order.buySell
                orderData['orderQuantity'] = order.quantity
                orderData['orderPrice'] = order.price
                orderData['matchedQuantity'] = order.filledQty
                orderData['matchedPrice'] = order.avgPrice
                orderData['unmatchedQuantity'] = order.quantity - order.filledQty
                orderData['orderStatus'] = order.orderStatus
                orderData['matchedValue'] = order.filledQty * order.avgPrice
                orderData['orderNumber'] = order.orderID
                orderData['uniqueID'] = order.uniqueID
                orderData['orderTime'] = order.inputTime
                orderData['orderType'] = order.orderType
            else:
                orderData = json.loads(orderData)
                orderData['orderQuantity'] = order.quantity
                orderData['orderPrice'] = order.price
                orderData['matchedQuantity'] = order.filledQty
                orderData['matchedPrice'] = order.avgPrice
                orderData['unmatchedQuantity'] = order.quantity - order.filledQty
                orderData['orderStatus'] = order.orderStatus
                orderData['matchedValue'] = order.filledQty * order.avgPrice
            
            
            if(order.avgPrice is not None and order.avgPrice > 0):
                orderData['lastPrice'] = order.avgPrice
                if(orderData.get('firstPrice', None) is None): orderData['firstPrice'] = order.avgPrice

            #Update to redis key. prepair for per redis key each order
            self.redis.set(perOrderRedisKey, json.dumps(orderData), self.redisTimeout)
            uniqueId = order.uniqueID
            perRequestRedisKey = f"stock_bot_request_{self.accountId}_{uniqueId}"    
            if(self.redis.get(perRequestRedisKey) is None):
                self.redis.set(perRequestRedisKey, json.dumps(order.toDict()), self.redisTimeout)


            

        
        

        
        
        
    