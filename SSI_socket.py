from signalr import Connection as BaseConnection
from signalr.events import EventHook
from signalr.hubs import Hub
from Console.Lib.SSI.SSI import SSIClient
import requests
from time import sleep
from time import time
from Console.Helper.Timer import *
from Console.Helper.Defaults import *
from Console.Helper.Redis import Redis

class Connection(BaseConnection):
    def __init__(self, url, session):
        super().__init__(url, session)
        self.on_close = EventHook()  # Thêm sự kiện mới
    
    def close(self):            
        super().close()
        self.on_close.fire()  # Gọi sự kiện on_close khi đóng

class SSIStream():
    def __init__(self, account_id, handle_message, handle_error, handle_exception=None, handle_close=None, notifyID = '0') -> None:
        self.client = SSIClient(account_id)
        self._stream_url = "https://fc-tradehub.ssi.com.vn/"
        self.message_handles = []
        self.message_handles.append(handle_message)
        self.error_handlers = []
        self.handle_close = handle_close
        self.handle_exception = handle_exception
        if handle_error is not None:
            self.error_handlers.append(handle_error)
        self.notifyID = str(notifyID)
        self.redis = Redis()
        self.redisNewEvent = f"stock_bot_new_event_{self.client.account.account_id}"
        self.is_connected = False

    def on_message(self, message):
        try:
            for fc_handler in self.message_handles:
                fc_handler(message)
        except Exception as e:
            exceptionInfo(e)
    
    def on_error(self, error):
        try:
            self.is_connected = False
            log(error)
            for fc_error_handler in self.error_handlers:
                fc_error_handler(error)
        except Exception as e:
            exceptionInfo(e)
    def on_close(self):
        try:
            self.is_connected = False
            if self.handle_close and callable(self.handle_close):
                self.handle_close()
            log("SSI Stream closed")
        except Exception as e:
            exceptionInfo(e)

    def start(self):
        self.client.getRedisAuth()
        if not self.client.authorized:
            raise Exception(f"SSI account[{self.client.account.account_name}] was not authorized!")
        with requests.Session() as session:
            st = time()
            self.is_connected = True
            session.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
            session.headers["Authorization"] = "Bearer " + self.client.access_token
            session.headers["NotifyID"] = self.notifyID
            
            self.connection = Connection(self._stream_url + 'v2.0/signalr', session)
            chat: Hub = self.connection.register_hub('BroadcastHubV2')
            chat.client.on('Broadcast', self.on_message)
            chat.client.on('Error', self.on_error)
            self.connection.error += self.on_error
            self.connection.on_close += self.on_close
            if self.handle_exception and callable(self.handle_exception):
                self.connection.exception += self.handle_exception
            self.connection.start()
            log("SSI Stream ready")
            log(f"SSI Stream Excuted: {time()-st}")
                # while True:
                #     # print(chat.client.name)
                #     sleep(10)
    
    def keep_alive(self):
        while True:
            sleep(10)

    def restart(self):
        self.stop()
        self.start()

    def stop(self):
        if self.is_connected:
            self.connection.close()
            self.is_connected = False
            