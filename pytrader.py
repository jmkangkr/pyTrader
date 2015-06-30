#!/usr/bin/env python
#
#                        pytrader.py
#
#
# Project page
#                           https://github.com/jmkangkr/pyTrader
#
# To clone the project
#             git clone https://github.com/jmkangkr/pyTrader.git
#
# Please send bugs to
#                                                   Jaemin Kang
#                                            jmkangkr@gmail.com
#

import win32com.client
import pythoncom
import getpass
from queue import Queue, Empty
import logging
import sys
import datetime
import os
import sqlite3
import threading
import time


VERSION = (0, 0)


RES_DIRECTORY   = "C:\\eBEST\\xingAPI\\Res"
NO_OCCURS       = 0

logger          = None

def simple_encode(key, clear):
    enc = []
    for i in range(len(clear)):
        key_c = key[i % len(key)]
        enc_c = chr((ord(clear[i]) + ord(key_c)) % 256)
        enc.append(enc_c)
    encoded = "".join(enc)
    return encoded


def simple_decode(key, enc):
    dec = []
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)


def setup_logger():
    global logger

    logger = logging.getLogger()

    formatter = logging.Formatter(fmt='%(asctime)s (%(levelname)s) %(message)s', datefmt='%Y%m%d %H:%M:%S')

    file_handler   = logging.FileHandler('{}.log'.format(os.path.splitext(sys.argv[0])[0]))
    stream_handler = logging.StreamHandler()

    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    #logging_level = logging.CRITICAL
    #logging_level = logging.ERROR
    #logging_level = logging.WARNING
    logging_level = logging.INFO
    #logging_level = logging.DEBUG

    logger.setLevel(logging_level)


def valid_date(s):
    try:
        date_parsed = datetime.datetime.strptime(s, "%Y%m%d")
    except ValueError:
        return None

    return date_parsed


def encrypt_login_information():
    (user_id, user_ps, user_pw) = get_login_information()
    key = getpass.getpass("Enter encryption key: ")
    login_string = '\t'.join((user_id, user_ps, user_pw))
    encoded = simple_encode(key, login_string)
    f = open('ud', "wb")
    f.write(encoded.encode('utf-8'))
    f.close()


def get_login_information():
    user_id = input("Enter user id: ")
    user_ps = getpass.getpass("Enter password for {}: ".format(user_id))
    user_pw = getpass.getpass("Enter password for certificate: ")

    return (user_id, user_ps, user_pw)


def preprocess_options():
    start_date = None
    end_date = None

    help = """\
usage: {} [-h] [-v] [-s] start_date [end_date]

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -s, --login-setup     creates user login data for easy login
  start_date            trading start date (format YYYYMMDD)
  end_date              trading end date (format YYYYMMDD)\
            """.format(sys.argv[0])

    for option in sys.argv[1:]:
        if option == '--help' or option == '-h':
            print(help)
            exit(0)
        elif option == '--version' or option ==  '-v':
            print("""\
{} version {}\
            """.format(sys.argv[0], '.'.join(map(str, VERSION))))
            exit(0)
        elif option == '--login-setup' or option == '-s':
            encrypt_login_information()
            print("Login data created")
            exit(0)
        else:
            date_parsed = valid_date(option)
            if date_parsed:
                if start_date:
                    end_date = date_parsed
                else:
                    start_date = date_parsed
            else:
                print("Not a valid date format.")
                exit(0)

    if not start_date or not end_date:
        print(help)
        exit(0)

    return (start_date, end_date)


class Logger(object):
    CRITICAL    = logging.CRITICAL
    ERROR       = logging.ERROR
    WARN        = logging.WARNING
    INFO        = logging.INFO
    DEBUG       = logging.DEBUG

    def __init__(self):
        super(Logger, self).__init__()

    def log(self, level, message):
        logger.log(level, '{}: {}'.format(self.__class__.__name__, message))


class Scheduler(Logger):
    def __init__(self):
        super(Scheduler, self).__init__()
        self.__runnables = []

    def registerRunnable(self, runnable):
        self.log(Logger.DEBUG, 'Adding runnable: {}'.format(runnable))
        self.__runnables.append(runnable)

    def run(self):
        try:
            while len(self.__runnables):
                #self.log(Logger.DEBUG, 'Runnable states: {}'.format([r.state for r in self.__runnables]))
                for runnable in list(self.__runnables):
                    if      runnable.state  ==  Runnable.INIT:

                        runnable.onStarted()
                        runnable.state = Runnable.RUNNING

                    elif    runnable.state  ==  Runnable.RUNNING:

                        handled = runnable.run()
                        if not handled:
                            runnable.onIdle()

                    elif    runnable.state  ==  Runnable.PAUSED:

                        runnable.onPaused()

                    elif    runnable.state  ==  Runnable.STOPPED:

                        runnable.onStopped()
                        runnable.state = Runnable.DEAD
                        self.__runnables.remove(runnable)

        except KeyboardInterrupt:
            exit(0)


class Runnable(Logger):
    INIT            = 0
    RUNNING         = 1
    PAUSED          = 2
    STOPPED         = 3
    DEAD            = 4

    def __init__(self):
        super(Runnable, self).__init__()
        self._state = Runnable.INIT

    def onStarted(self):
        raise NotImplementedError

    def onPaused(self):
        raise NotImplementedError

    def onStopped(self):
        raise NotImplementedError

    def onIdle(self):
        raise NotImplementedError

    def run(self):
        raise NotImplementedError

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value


class XAMessageQueue(Runnable):
    def __init__(self):
        super(XAMessageQueue, self).__init__()
        self.__message_queue = Queue()
        self.__handlers = {}

    def addHandler(self, message, handler):
        self.log(Logger.DEBUG, 'Registering {} for {}'.format(handler, message))
        self.__handlers[message] = handler

    def sendMessage(self, message, parameter):
        self.log(Logger.INFO, 'sendMessage: {}'.format(message))
        self.__message_queue.put((message, parameter))

    def onIdle(self):
        pythoncom.PumpWaitingMessages()

    def run(self):
        try:
            (message, parameter) = self.__message_queue.get(False)
            self.log(Logger.DEBUG, 'Message received: {}({})'.format(message, parameter))
            self.__handlers[message](self, parameter)
        except Empty:
            return False

        return True


class XAApplication(XAMessageQueue):
    LOGGED_ON       = "LOGGED_ON"
    LOGGED_OUT      = "LOGGED_OUT"
    DISCONNECTED    = "DISCONNECTED"

    def __init__(self, strategy, feeder):
        super(XAApplication, self).__init__()
        self.__strategy = strategy
        self.__feeder = feeder

        self.addHandler(XAApplication.LOGGED_ON, XAApplication.onLoggedOn)
        self.addHandler(XAApplication.LOGGED_OUT, XAApplication.onLoggedOut)
        self.addHandler(XAApplication.DISCONNECTED, XAApplication.onDisconnected)

    class XASessionEvents(Logger):
        def __init__(self):
            super(XAApplication.XASessionEvents, self).__init__()
            self.__application = None

        def postInitialize(self, application):
            self.__application = application

        def OnLogin(self, errorCode, errorDesc):
            self.log(Logger.INFO, 'OnLogin')
            if errorCode != '0000':
                self.log(ERROR, "Error {}: {}".format(errorCode, errorDesc))
            self.__application.sendMessage(XAApplication.LOGGED_ON, errorCode == '0000')

        def OnLogout(self):
            self.log(Logger.INFO, 'OnLogout')
            pass

        def OnDisconnect(self):
            self.log(Logger.INFO, 'OnDisconnect')
            pass

    def onStarted(self):
        self.log(Logger.INFO, 'onStarted')

        server_addr_demo = "demo.ebestsec.co.kr"
        server_addr_real = "hts.ebestsec.co.kr"
        server_port = 20001
        server_type = 0

        try:
            cipher = open('ud', 'rb')
            encoded = cipher.read()
            key = getpass.getpass("Enter decryption key: ")
            decoded = simple_decode(key, encoded.decode('utf-8'))
            (user_id, user_ps, user_pw) = decoded.split('\t')
        except IOError:
            (user_id, user_ps, user_pw) = get_login_information()

        self.__xasession = win32com.client.DispatchWithEvents("XA_Session.XASession", XAApplication.XASessionEvents)
        self.__xasession.postInitialize(self)
        success = self.__xasession.ConnectServer(server_addr_real, server_port)
        if not success:
            errorCode = self.__xasession.GetLastError()
            errorDesc = self.__xasession.GetErrorMessage(errorCode)
            logger.error("Error {}: {}".format(errorCode, errorDesc))
            self.sendMessage(XAApplication.LOGGED_ON, False)

        success = self.__xasession.Login(user_id, user_ps, user_pw, server_type, 0)

        if not success:
            errorCode = self.__xasession.GetLastError()
            errorDesc = self.__xasession.GetErrorMessage(errorCode)
            logger.error("Error {}: {}".format(errorCode, errorDesc))
            self.sendMessage(XAApplication.LOGGED_ON, False)

        return True

    def onPaused(self):
        self.log(Logger.INFO, 'onPaused')

    def onStopped(self):
        self.log(Logger.INFO, 'onStopped')

    def onLoggedOn(self, success):
        self.log(Logger.INFO, 'onLoggedOn')

        if not success:
            self.error('Login failed')
            return

        db = XADatabaseDay(["000150", "005930"])

        db.updateDatabase()

        self.__strategy.onTradeStart()
        self.__feeder.startFeed()

    def onLoggedOut(self, param):
        self.log(Logger.INFO, 'onLoggedOut')

    def onDisconnected(self, param):
        self.log(Logger.INFO, 'onDisconnected')


class XADataFeederBase(XAMessageQueue):
    DATA_RECEIVED       = "DATA_RECEIVED"

    def __init__(self, strategy):
        super(XADataFeederBase, self).__init__()
        self._state = Runnable.PAUSED
        self.__strategy = strategy

        self.addHandler(XADataFeederBase.DATA_RECEIVED, XADataFeederBase.onDataReceived)

    class XAQueryEvents(Logger):
        def __init__(self):
            super(XADataFeederBase.XAQueryEvents, self).__init__()

        def postInitialize(self, feeder):
            self.__feeder = feeder

        def OnReceiveData(self, szTrCode):
            self.log(Logger.DEBUG, "OnReceiveData: szTrCode({})".format(szTrCode))
            self.__feeder.sendMessage(XADataFeederBase.DATA_RECEIVED, szTrCode)

        def OnReceiveMessage(self, systemError, messageCode, message):
            self.log(Logger.DEBUG, "OnReceiveMessage: systemError({}), messageCode({}), message({})".format(systemError, messageCode, message))

    def startFeed(self):
        self._state = Runnable.INIT

    def onPaused(self):
        pass

    def onDataReceived(self, param):
        bar = self.translateData(param)
        self.__strategy.onBar(bar)

    def translateData(self, param):
        raise NotImplementedError


class XADataFeederDay(XADataFeederBase):
    def __init__(self, strategy, start_date, end_date, stock_code):
        super(XADataFeederDay, self).__init__(strategy)
        self.__start_date = start_date
        self.__end_date = end_date
        self.__stock_code = stock_code
        self.__xaquery = None
        self.__conn = None
        self.__cur = None

    def _checkDatabase(self):
        db_name = '{}.db'.format(os.path.splitext(sys.argv[0])[0])
        return os.path.isfile(db_name)

    def _openDatabase(self):
        db_name = '{}.db'.format(os.path.splitext(sys.argv[0])[0])
        self.__conn = sqlite3.connect(db_name)
        self.__cur = self.__conn.cursor()

    def _createDatabase(self):
        db_name = '{}.db'.format(os.path.splitext(sys.argv[0])[0])
        self.__conn = sqlite3.connect(db_name)
        self.__cur = self.__conn.cursor()

        sqlcommand = "CREATE TABLE t1305 "                 \
                               "(date            TEXT,    "\
                                "open            INTEGER, "\
                                "high            INTEGER, "\
                                "low             INTEGER, "\
                                "close           INTEGER, "\
                                "sign            TEXT,    "\
                                "change          INTEGER, "\
                                "diff            REAL,    "\
                                "volume          INTEGER, "\
                                "diff_vol        REAL,    "\
                                "chdegree        REAL,    "\
                                "sojinrate       REAL,    "\
                                "changerate      REAL,    "\
                                "fpvolume        INTEGER, "\
                                "covolume        INTEGER, "\
                                "shcode          TEXT,    "\
                                "value           INTEGER, "\
                                "ppvolume        INTEGER, "\
                                "o_sign          TEXT,    "\
                                "o_change        INTEGER, "\
                                "o_diff          REAL,    "\
                                "h_sign          TEXT,    "\
                                "h_change        INTEGER, "\
                                "h_diff          REAL,    "\
                                "l_sign          TEXT,    "\
                                "l_change        INTEGER, "\
                                "l_diff          REAL,    "\
                                "marketcap       INTEGER) "

        self.__cur.execute(sqlcommand)
        self.__conn.commit()
        self.__conn.close()

    def onStarted(self):
        self.log(Logger.INFO, 'onStarted')

        if not self._checkDatabase():
            self._createDatabase()
        else:
            self._openDatabase()

        self.__xaquery = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XADataFeederBase.XAQueryEvents)
        self.__xaquery.postInitialize(self)
        self.__xaquery.LoadFromResFile(os.path.join(RES_DIRECTORY, 't1305.res'))
        self.__xaquery.SetFieldData('t1305InBlock', 'shcode', NO_OCCURS, self.__stock_code)
        self.__xaquery.SetFieldData('t1305InBlock', 'dwmcode', NO_OCCURS, 1)
        self.__xaquery.SetFieldData('t1305InBlock', 'cnt', NO_OCCURS, (self.__end_date - self.__start_date).TotalDays)
        self.__xaquery.Request(0)

    def translateData(self, param):
        self.log(Logger.INFO, 'translateData')

        print('Total days: {}'.format((self.__end_date - self.__start_date).TotalDays))
        for i in range(0, (self.__end_date - self.__start_date).TotalDays):
            date            = self.__xaquery.GetFieldData("t1305OutBlock1", "date",       i)
            open            = self.__xaquery.GetFieldData("t1305OutBlock1", "open",       i)
            high            = self.__xaquery.GetFieldData("t1305OutBlock1", "high",       i)
            low             = self.__xaquery.GetFieldData("t1305OutBlock1", "low",        i)
            close           = self.__xaquery.GetFieldData("t1305OutBlock1", "close",      i)
            sign            = self.__xaquery.GetFieldData("t1305OutBlock1", "sign",       i)
            change          = self.__xaquery.GetFieldData("t1305OutBlock1", "change",     i)
            diff            = self.__xaquery.GetFieldData("t1305OutBlock1", "diff",       i)
            volume          = self.__xaquery.GetFieldData("t1305OutBlock1", "volume",     i)
            diff_vol        = self.__xaquery.GetFieldData("t1305OutBlock1", "diff_vol",   i)
            chdegree        = self.__xaquery.GetFieldData("t1305OutBlock1", "chdegree",   i)
            sojinrate       = self.__xaquery.GetFieldData("t1305OutBlock1", "sojinrate",  i)
            changerate      = self.__xaquery.GetFieldData("t1305OutBlock1", "changerate", i)
            fpvolume        = self.__xaquery.GetFieldData("t1305OutBlock1", "fpvolume",   i)
            covolume        = self.__xaquery.GetFieldData("t1305OutBlock1", "covolume",   i)
            shcode          = self.__xaquery.GetFieldData("t1305OutBlock1", "shcode",     i)
            value           = self.__xaquery.GetFieldData("t1305OutBlock1", "value",      i)
            ppvolume        = self.__xaquery.GetFieldData("t1305OutBlock1", "ppvolume",   i)
            o_sign          = self.__xaquery.GetFieldData("t1305OutBlock1", "o_sign",     i)
            o_change        = self.__xaquery.GetFieldData("t1305OutBlock1", "o_change",   i)
            o_diff          = self.__xaquery.GetFieldData("t1305OutBlock1", "o_diff",     i)
            h_sign          = self.__xaquery.GetFieldData("t1305OutBlock1", "h_sign",     i)
            h_change        = self.__xaquery.GetFieldData("t1305OutBlock1", "h_change",   i)
            h_diff          = self.__xaquery.GetFieldData("t1305OutBlock1", "h_diff",     i)
            l_sign          = self.__xaquery.GetFieldData("t1305OutBlock1", "l_sign",     i)
            l_change        = self.__xaquery.GetFieldData("t1305OutBlock1", "l_change",   i)
            l_diff          = self.__xaquery.GetFieldData("t1305OutBlock1", "l_diff",     i)
            marketcap       = self.__xaquery.GetFieldData("t1305OutBlock1", "marketcap",  i)

        return (date, open, high, low, close)


class StrategyBase(Logger):
    def __init__(self):
        super(StrategyBase, self).__init__()

    def onTradeStart(self):
        raise NotImplementedError

    def onBar(self, param):
        raise NotImplementedError

class MyStrategy(StrategyBase):
    def __init__(self):
        super(MyStrategy, self).__init__()

    def onTradeStart(self):
        self.log(Logger.INFO, 'onTradeStart')

    def onBar(self, param):
        self.log(Logger.INFO, 'onBar')
        (val_date, val_open, val_high, val_low, val_close) = param
        print('{}: {}, {}, {}, {}'.format(val_date, val_open, val_high, val_low, val_close))


class XAData_t1305(object):
    def __init__(self, data_set):
        super(XAData_t1305, self).__init__()
        (date, open, high, low, close, sign, change, diff, volume, diff_vol, chdegree, sojinrate, changerate, fpvolume, covolume, shcode, value, ppvolume, o_sign, o_change, o_diff, h_sign, h_change, h_diff, l_sign, l_change, l_diff, marketcap) = data_set
        self.__val_date            = date
        self.__val_open            = open
        self.__val_high            = high
        self.__val_low             = low
        self.__val_close           = close
        self.__val_sign            = sign
        self.__val_change          = change
        self.__val_diff            = diff
        self.__val_volume          = volume
        self.__val_diff_vol        = diff_vol
        self.__val_chdegree        = chdegree
        self.__val_sojinrate       = sojinrate
        self.__val_changerate      = changerate
        self.__val_fpvolume        = fpvolume
        self.__val_covolume        = covolume
        self.__val_shcode          = shcode
        self.__val_value           = value
        self.__val_ppvolume        = ppvolume
        self.__val_o_sign          = o_sign
        self.__val_o_change        = o_change
        self.__val_o_diff          = o_diff
        self.__val_h_sign          = h_sign
        self.__val_h_change        = h_change
        self.__val_h_diff          = h_diff
        self.__val_l_sign          = l_sign
        self.__val_l_change        = l_change
        self.__val_l_diff          = l_diff
        self.__val_marketcap       = marketcap

    def __str__(self):
        return "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(self.__val_date, self.__val_open, self.__val_high, self.__val_low, self.__val_close, self.__val_sign, self.__val_change, self.__val_diff, self.__val_volume, self.__val_diff_vol, self.__val_chdegree, self.__val_sojinrate, self.__val_changerate, self.__val_fpvolume, self.__val_covolume, self.__val_shcode, self.__val_value, self.__val_ppvolume, self.__val_o_sign, self.__val_o_change, self.__val_o_diff, self.__val_h_sign, self.__val_h_change, self.__val_h_diff, self.__val_l_sign, self.__val_l_change, self.__val_l_diff, self.__val_marketcap)

    @property
    def date(self):
        return self.__val_date

    @property
    def open(self):
        return self.__val_open

    @property
    def high(self):
        return self.__val_high

    @property
    def low(self):
        return self.__val_low

    @property
    def close(self):
        return self.__val_close

    @property
    def sign(self):
        return self.__val_sign

    @property
    def change(self):
        return self.__val_change

    @property
    def diff(self):
        return self.__val_diff

    @property
    def volume(self):
        return self.__val_volume

    @property
    def diff_vol(self):
        return self.__val_diff_vol

    @property
    def chdegree(self):
        return self.__val_chdegree

    @property
    def sojinrate(self):
        return self.__val_sojinrate

    @property
    def changerate(self):
        return self.__val_changerate

    @property
    def fpvolume(self):
        return self.__val_fpvolume

    @property
    def covolume(self):
        return self.__val_covolume

    @property
    def shcode(self):
        return self.__val_shcode

    @property
    def value(self):
        return self.__val_value

    @property
    def ppvolume(self):
        return self.__val_ppvolume

    @property
    def o_sign(self):
        return self.__val_o_sign

    @property
    def o_change(self):
        return self.__val_o_change

    @property
    def o_diff(self):
        return self.__val_o_diff

    @property
    def h_sign(self):
        return self.__val_h_sign

    @property
    def h_change(self):
        return self.__val_h_change

    @property
    def h_diff(self):
        return self.__val_h_diff

    @property
    def l_sign(self):
        return self.__val_l_sign

    @property
    def l_change(self):
        return self.__val_l_change

    @property
    def l_diff(self):
        return self.__val_l_diff

    @property
    def marketcap(self):
        return self.__val_marketcap


class XADatabaseDay(Logger):
    BEGINNING = datetime.date(2008, 1, 1)

    def __init__(self, stocks):
        super(XADatabaseDay, self).__init__()
        self.__xaquery = None
        self.__stocks = stocks
        self.__conn = sqlite3.connect('{}.db'.format(os.path.splitext(sys.argv[0])[0]))
        self.__cur = self.__conn.cursor()

    class XAQueryEvents(Logger):
        def __init__(self):
            super(XADatabaseDay.XAQueryEvents, self).__init__()
            self.__signal = threading.Semaphore()

        def postInitialize(self):
            self.__signal.acquire(True)

        def OnReceiveData(self, szTrCode):
            self.log(Logger.DEBUG, "OnReceiveData: szTrCode({})".format(szTrCode))
            self.__signal.release()

        def OnReceiveMessage(self, systemError, messageCode, message):
            self.log(Logger.DEBUG, "OnReceiveMessage: systemError({}), messageCode({}), message({})".format(systemError, messageCode, message))

        def waitData(self):
            while not self.__signal.acquire(False):
                time.sleep(0.1)
                pythoncom.PumpWaitingMessages()

    def _createDatabase(self):
        for stock in self.__stocks:
            self.log(Logger.INFO, "Creating database for {} if not exists.".format(stock))
            sqlcommand = "CREATE TABLE IF NOT EXISTS t1305_{} ("                       \
                                    "date            TEXT     UNIQUE,   "\
                                    "open            INTEGER,           "\
                                    "high            INTEGER,           "\
                                    "low             INTEGER,           "\
                                    "close           INTEGER,           "\
                                    "sign            TEXT,              "\
                                    "change          INTEGER,           "\
                                    "diff            REAL,              "\
                                    "volume          INTEGER,           "\
                                    "diff_vol        REAL,              "\
                                    "chdegree        REAL,              "\
                                    "sojinrate       REAL,              "\
                                    "changerate      REAL,              "\
                                    "fpvolume        INTEGER,           "\
                                    "covolume        INTEGER,           "\
                                    "shcode          TEXT,              "\
                                    "value           INTEGER,           "\
                                    "ppvolume        INTEGER,           "\
                                    "o_sign          TEXT,              "\
                                    "o_change        INTEGER,           "\
                                    "o_diff          REAL,              "\
                                    "h_sign          TEXT,              "\
                                    "h_change        INTEGER,           "\
                                    "h_diff          REAL,              "\
                                    "l_sign          TEXT,              "\
                                    "l_change        INTEGER,           "\
                                    "l_diff          REAL,              "\
                                    "marketcap       INTEGER            "\
                                                           ")".format(stock)
            self.__cur.execute(sqlcommand)
        self.__conn.commit()

    def updateDatabase(self):
        self._createDatabase()

        for stock in self.__stocks:
            last_data = self.lastData(stock)

            if not last_data:
                last_date = XADatabaseDay.BEGINNING
            else:
                last_date = datetime.datetime.strptime(last_data.date, "%Y%m%d").date()

            days_to_request = int((datetime.date.today() - last_date).days * (5.0/7.0) + 7)

            self.log(Logger.INFO, "Updating {} - last date{}, requesting {} days of data".format(stock, last_date, days_to_request))

            self.__xaquery = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XADatabaseDay.XAQueryEvents)
            self.__xaquery.postInitialize()
            self.__xaquery.LoadFromResFile(os.path.join(RES_DIRECTORY, 't1305.res'))
            self.__xaquery.SetFieldData('t1305InBlock', 'shcode', NO_OCCURS, stock)
            self.__xaquery.SetFieldData('t1305InBlock', 'dwmcode', NO_OCCURS, 1)
            self.__xaquery.SetFieldData('t1305InBlock', 'cnt', NO_OCCURS, days_to_request)
            result = self.__xaquery.Request(0)

            print("waitData: {}".format(result))
            self.__xaquery.waitData()
            print("waitData Done")

            for i in range(0, days_to_request):
                val_date            = self.__xaquery.GetFieldData("t1305OutBlock1", "date",       i)
                val_open            = self.__xaquery.GetFieldData("t1305OutBlock1", "open",       i)
                val_high            = self.__xaquery.GetFieldData("t1305OutBlock1", "high",       i)
                val_low             = self.__xaquery.GetFieldData("t1305OutBlock1", "low",        i)
                val_close           = self.__xaquery.GetFieldData("t1305OutBlock1", "close",      i)
                val_sign            = self.__xaquery.GetFieldData("t1305OutBlock1", "sign",       i)
                val_change          = self.__xaquery.GetFieldData("t1305OutBlock1", "change",     i)
                val_diff            = self.__xaquery.GetFieldData("t1305OutBlock1", "diff",       i)
                val_volume          = self.__xaquery.GetFieldData("t1305OutBlock1", "volume",     i)
                val_diff_vol        = self.__xaquery.GetFieldData("t1305OutBlock1", "diff_vol",   i)
                val_chdegree        = self.__xaquery.GetFieldData("t1305OutBlock1", "chdegree",   i)
                val_sojinrate       = self.__xaquery.GetFieldData("t1305OutBlock1", "sojinrate",  i)
                val_changerate      = self.__xaquery.GetFieldData("t1305OutBlock1", "changerate", i)
                val_fpvolume        = self.__xaquery.GetFieldData("t1305OutBlock1", "fpvolume",   i)
                val_covolume        = self.__xaquery.GetFieldData("t1305OutBlock1", "covolume",   i)
                val_shcode          = self.__xaquery.GetFieldData("t1305OutBlock1", "shcode",     i)
                val_value           = self.__xaquery.GetFieldData("t1305OutBlock1", "value",      i)
                val_ppvolume        = self.__xaquery.GetFieldData("t1305OutBlock1", "ppvolume",   i)
                val_o_sign          = self.__xaquery.GetFieldData("t1305OutBlock1", "o_sign",     i)
                val_o_change        = self.__xaquery.GetFieldData("t1305OutBlock1", "o_change",   i)
                val_o_diff          = self.__xaquery.GetFieldData("t1305OutBlock1", "o_diff",     i)
                val_h_sign          = self.__xaquery.GetFieldData("t1305OutBlock1", "h_sign",     i)
                val_h_change        = self.__xaquery.GetFieldData("t1305OutBlock1", "h_change",   i)
                val_h_diff          = self.__xaquery.GetFieldData("t1305OutBlock1", "h_diff",     i)
                val_l_sign          = self.__xaquery.GetFieldData("t1305OutBlock1", "l_sign",     i)
                val_l_change        = self.__xaquery.GetFieldData("t1305OutBlock1", "l_change",   i)
                val_l_diff          = self.__xaquery.GetFieldData("t1305OutBlock1", "l_diff",     i)
                val_marketcap       = self.__xaquery.GetFieldData("t1305OutBlock1", "marketcap",  i)

                date = datetime.datetime.strptime(val_date, "%Y%m%d").date()

                if date >= XADatabaseDay.BEGINNING:
                    sqlcommand = "INSERT OR IGNORE INTO t1305_{} (date, open, high, low, close, sign, change, diff, volume, diff_vol, chdegree, sojinrate, changerate, fpvolume, covolume, shcode, value, ppvolume, o_sign, o_change, o_diff, h_sign, h_change, h_diff, l_sign, l_change, l_diff, marketcap) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(stock, val_date, val_open, val_high, val_low, val_close, val_sign, val_change, val_diff, val_volume, val_diff_vol, val_chdegree, val_sojinrate, val_changerate, val_fpvolume, val_covolume, val_shcode, val_value, val_ppvolume, val_o_sign, val_o_change, val_o_diff, val_h_sign, val_h_change, val_h_diff, val_l_sign, val_l_change, val_l_diff, val_marketcap)
                    self.__cur.execute(sqlcommand)
                    self.log(Logger.INFO, "Inserting {} data into database".format(val_date))

            self.__conn.commit()

    def lastData(self, stock):
        sqlcommand = "SELECT * FROM t1305_{} ORDER BY date DESC LIMIT 1".format(stock)
        self.__cur.execute(sqlcommand)
        result = self.__cur.fetchone()

        if not result:
            return None

        return XAData_t1305(result)

    def data(self, stock, date):
        date_string = datetime.datetime.strftime(date, "%Y%m%d")
        sqlcommand = "SELECT * FROM t1305_{} WHERE date LIKE {}".format(stock, date_string)
        self.__cur.execute(sqlcommand)
        result = self.__cur.fetchone()

        if not result:
            return None

        return XAData_t1305(result)



def main():
    setup_logger()

    (start_date, end_date) = preprocess_options()

    sched = Scheduler()

    strategy = MyStrategy()

    feeder = XADataFeederDay(strategy, start_date, end_date, "000150")

    app = XAApplication(strategy, feeder)

    sched.registerRunnable(app)
    sched.registerRunnable(feeder)

    sched.run()

if __name__ == "__main__":
    main()
