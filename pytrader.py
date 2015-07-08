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
import time


VERSION = (0, 0)


RES_DIRECTORY   = "C:\\eBEST\\xingAPI\\Res"
NO_OCCURS       = 0

logger          = None


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
    #logging_level = logging.INFO
    logging_level = logging.DEBUG

    logger.setLevel(logging_level)


def valid_date(s):
    try:
        date_parsed = datetime.datetime.strptime(s, "%Y%m%d")
    except ValueError:
        return None

    return date_parsed

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

    'FIXME - method name'
    @classmethod
    def log2(cls, level, message, classname):
        logger.log(level, '{}: {}'.format(classname, message))


class XAScheduler(Logger):
    __message_queue = Queue()
    __timers = []

    def __init__(self):
        super(XAScheduler, self).__init__()
        self.__runnables = []

    def registerRunnable(self, runnable):
        self.log(Logger.DEBUG, 'Registering runnable: {}'.format(runnable))
        self.__runnables.append(runnable)

    @classmethod
    def registerTimer(cls, target, parameter, seconds):
        Logger.log2(Logger.DEBUG, 'Registering timer for {} after {} seconds'.format(target, seconds), target)
        XAScheduler.__timers.append((time.time(), target, parameter, seconds))

    @classmethod
    def sendMessage(cls, target, message, outparam, inparam, sender):
        Logger.log2(Logger.DEBUG, 'Send message to {} from {}: msg({})'.format(target, sender, message), sender)
        XAScheduler.__message_queue.put((target, message, outparam, inparam, sender))

    def run(self):
        try:
            while len(self.__runnables):
                for runnable in list(self.__runnables):
                    if      runnable.state  ==  XARunnable.STAT_INIT:
                        self.log(Logger.DEBUG, 'Starting {}'.format(runnable))
                        runnable.onMessage(XARunnable.MSG_STARTED, None, None, self)
                        runnable.state = XARunnable.STAT_RUNNING

                    elif    runnable.state  ==  XARunnable.STAT_PAUSED:
                        self.log(Logger.DEBUG, 'Pausing {}'.format(runnable))
                        runnable.onMessage(XARunnable.MSG_PAUSED, None, None, self)

                    elif    runnable.state  ==  XARunnable.STAT_STOPPED:
                        self.log(Logger.DEBUG, 'Stopping {}'.format(runnable))
                        runnable.onMessage(XARunnable.MSG_STOPPED, None, None, self)
                        runnable.state = XARunnable.STAT_DEAD
                        self.__runnables.remove(runnable)

                for timer in list(XAScheduler.__timers):
                    (trigger, target, parameter, seconds) = timer
                    if seconds >= (time.time() - trigger):
                        XAScheduler.__timers.remove(timer)
                        XAScheduler.sendMessage(target, XARunnable.MSG_TIMER, None, parameter, self)

                try:
                    (target, message, outparam, inparam, sender) = XAScheduler.__message_queue.get(False)
                    self.log(Logger.DEBUG, 'MSG: target({}), msg({})'.format(target.__class__.__name__, message))
                    target.onMessage(message, outparam, inparam, sender)
                except Empty:
                    pythoncom.PumpWaitingMessages()

        except KeyboardInterrupt:
            exit(0)


class XARunnable(Logger):
    STAT_INIT            = 0
    STAT_RUNNING         = 1
    STAT_PAUSED          = 2
    STAT_STOPPED         = 3
    STAT_DEAD            = 4

    MSG_STARTED     = 'MSG_STARTED'
    MSG_PAUSED      = 'MSG_PAUSED'
    MSG_STOPPED     = 'MSG_STOPPED'
    MSG_TIMER       = 'MSG_TIMER'

    def __init__(self):
        super(XARunnable, self).__init__()
        self._state = XARunnable.STAT_INIT

    def sendMessage(self, target, message, outparam, inparam):
        XAScheduler.sendMessage(target, message, outparam, inparam, self)

    def sleep(self, seconds, param):
        XAScheduler.registerTimer(self, param, seconds)

    def onMessage(self, message, outparam, inparam, sender):
        raise NotImplementedError

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value


class XASessionEvents(Logger):
    MSG_LOGGED_ON = 'MSG_LOGGED_ON'
    MSG_LOGGED_OUT = 'MSG_LOGGED_OUT'
    MSG_DISCONNECTED = 'MSG_DISCONNECTED'

    def __init__(self):
        super(XASessionEvents, self).__init__()
        self.__listener = None

    def postInitialize(self, listener):
        self.__listener = listener

    def OnLogin(self, errorCode, errorDesc):
        self.log(Logger.DEBUG, 'XASessionEvents:OnLogin')
        if errorCode != '0000':
            self.log(Logger.ERROR, "Login failed: {}".format(errorDesc))
        XAScheduler.sendMessage(self.__listener, XASessionEvents.MSG_LOGGED_ON, errorCode == '0000', None, self)

    def OnLogout(self):
        self.log(Logger.DEBUG, 'XASessionEvents:OnLogout')
        XAScheduler.sendMessage(self.__listener, XASessionEvents.MSG_LOGGED_OUT, None, None, self)

    def OnDisconnect(self):
        self.log(Logger.DEBUG, 'XASessionEvents:OnDisconnect')
        XAScheduler.sendMessage(self.__listener, XASessionEvents.MSG_DISCONNECTED, None, None, self)


class XAStrategyBase(XARunnable):
    def __init__(self, feeder):
        super(XAStrategyBase, self).__init__()
        self.__xasession = None
        self.__feeder = feeder

    def onMessage(self, message, outparam, inparam, sender):
        if message == XARunnable.MSG_STARTED:
            success = self.__login()
            if not success:
                self.log(Logger.ERROR, "Login request was not made successfully.")
        elif message == XARunnable.MSG_PAUSED:
            pass
        elif message == XARunnable.MSG_STOPPED:
            pass
        elif message == XASessionEvents.MSG_LOGGED_ON:
            success = outparam
            if not success:
                self.log(Logger.ERROR, "Login was not successful. Try it again.")
                self.__login()
                return

            self.__feeder.startFeed()
            success = self.__feeder.nextFeed(self, None)
            if not success:
                self.log(Logger.ERROR, "Data feeding failed.")
        elif message == XASessionEvents.MSG_LOGGED_OUT:
            pass
        elif message == XASessionEvents.MSG_DISCONNECTED:
            pass
        elif message == XADataFeederDay.MSG_DATA_FED:
            dataset = outparam

            if not dataset:
                print('End of feeding')
            else:
                print(dataset.date)

            if not dataset:
                success = self.__feeder.nextFeed(self, None)
                if not success:
                    self.log(Logger.ERROR, "Data feeding failed.")


    def __login(self):
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

        self.__xasession = win32com.client.DispatchWithEvents("XA_Session.XASession", XASessionEvents)
        self.__xasession.postInitialize(self)
        success = self.__xasession.ConnectServer(server_addr_real, server_port)
        if not success:
            errorCode = self.__xasession.GetLastError()
            errorDesc = self.__xasession.GetErrorMessage(errorCode)
            logger.error("Error {}: {}".format(errorCode, errorDesc))
            return False

        success = self.__xasession.Login(user_id, user_ps, user_pw, server_type, 0)

        if not success:
            errorCode = self.__xasession.GetLastError()
            errorDesc = self.__xasession.GetErrorMessage(errorCode)
            logger.error("Error {}: {}".format(errorCode, errorDesc))
            return False

        return True

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


class XADataFeederBase(XARunnable):
    def __init__(self):
        super(XADataFeederBase, self).__init__()

    def startFeed(self):
        raise NotImplementedError

    def nextFeed(self, callback, param):
        raise NotImplementedError


class XADataFeederDay(XADataFeederBase):
    MSG_DATA_FED = 'MSG_DATA_FED'

    def __init__(self, stock, start, end):
        super(XADataFeederDay, self).__init__()
        self.__stock = stock
        self.__start = start
        self.__end = end
        self.__current = None
        self.__database = None
        self.__fetcher = None

    def startFeed(self):
        self.log(Logger.INFO, "XADataFeederDay:startFeed called")
        self.__current = self.__start
        self.__database = XADatabaseDay([self.__stock])
        self.__fetcher = XADataFetcherDay()

        self.__database.updateDatabase()

        self.__database.initFetch(self.__stock, self.__start)
        return self

    def nextFeed(self, callback, param):
        self.log(Logger.INFO, "XADataFeederDay:nextFeed called")
        if self.__current > self.__end:
            self.sendMessage(callback, XADataFeederDay.MSG_DATA_FED, None, param)
            return True

        dataset = self.__database.fetch()

        if dataset:
            date = datetime.datetime.strptime(dataset.date, "%Y%m%d")
            self.__current = date + datetime.timedelta(days=1)
            if date > self.__end:
                self.sendMessage(callback, XADataFeederDay.MSG_DATA_FED, None, param)
                return True

            self.sendMessage(callback, XADataFeederDay.MSG_DATA_FED, dataset, param)
            return True

        days_to_request = int((datetime.date.today() - self.__current).days * (XADatabaseDay.WEEKDAYS_IN_WEEK / XADatabaseDay.DAYS_IN_WEEK) + XADatabaseDay.DAYS_IN_WEEK)
        success = self.__fetcher.fetch(self.__stock, days_to_request, self, (callback, param))
        if not success:
            return False

        return True

    def onMessage(self, message, outparam, inparam, sender):
        if message == XADataFeederDay.MSG_DATA_FED:
            datasets = outparam
            (callback, param) = inparam

            dataset_found = None
            for dataset in datasets:
                date = datetime.datetime.strptime(dataset.date, "%Y%m%d")
                if date >= self.__current:
                    dataset_found = dataset
                    break

            if dataset_found:
                date = datetime.datetime.strptime(dataset_found.date, "%Y%m%d")
                self.__current = date + datetime.timedelta(days=1)
                if date > self.__end:
                    self.sendMessage(callback, XADataFeederDay.MSG_DATA_FED, None, param)
                    return

                self.sendMessage(callback, XADataFeederDay.MSG_DATA_FED, dataset_found, param)
                return

            self.log(Logger.INFO, "Data is not available. Waiting some time and will try it later. Sleeping.")
            self.sleep(3600, (callback, param))

        elif message == XARunnable.MSG_TIMER:
            (callback, param) = inparam
            self.nextFeed(callback, param)


class XAQueryEvents(Logger):
    MSG_DATA_RECEIVED = 'MSG_DATA_RECEIVED'

    def __init__(self):
        super(XAQueryEvents, self).__init__()
        self.__listener = None
        self.__param = None

    def postInitialize(self, listener, param):
        self.__listener = listener
        self.__param = param

    def OnReceiveData(self, szTrCode):
        self.log(Logger.DEBUG, "XAQueryEvents:OnReceiveData - szTrCode({})".format(szTrCode))
        XAScheduler.sendMessage(self.__listener, XAQueryEvents.MSG_DATA_RECEIVED, None, self.__param, self)

    def OnReceiveMessage(self, systemError, messageCode, message):
        self.log(Logger.DEBUG, "XAQueryEvents:OnReceiveMessage - systemError({}), messageCode({}), message({})".format(systemError, messageCode, message))


class XADataFetcherDay(XARunnable):
    MSG_DATA_FETCHED = 'MSG_DATA_FETCHED'

    TIME_SENTINEL_ZERO = 0.0
    T1305_REQUEST_TIME_LIMIT = 1.0

    def __init__(self):
        super(XADataFetcherDay, self).__init__()
        self.__timeLastRequest = XADataFetcherDay.TIME_SENTINEL_ZERO
        self.__xaQueries = []

    def __del__(self):
        pass

    def __waitAndRequest(self, xaquery):
        time_to_sleep = XADataFetcherDay.T1305_REQUEST_TIME_LIMIT - (time.time() - self.__timeLastRequest)

        if time_to_sleep > 0:
            self.log(Logger.DEBUG, "Delaying request by {} second".format(time_to_sleep))
            time.sleep(time_to_sleep)

        result = xaquery.Request(0)
        self.__timeLastRequest = time.time()
        if result < 0:
            self.log(Logger.ERROR, "Request error: {}".format(result))
            return False

        return True

    def fetch(self, stock, days, callback, param):
        self.log(Logger.INFO, 'XADataFetcherDay:fetch called')
        xaquery = win32com.client.DispatchWithEvents("XA_DataSet.XAQuery", XAQueryEvents)
        xaquery.postInitialize(self, (stock, days, callback, param))
        xaquery.LoadFromResFile(os.path.join(RES_DIRECTORY, 't1305.res'))
        xaquery.SetFieldData('t1305InBlock', 'shcode', NO_OCCURS, stock)
        xaquery.SetFieldData('t1305InBlock', 'dwmcode', NO_OCCURS, 1)
        xaquery.SetFieldData('t1305InBlock', 'cnt', NO_OCCURS, days)

        self.log(Logger.INFO, "Requesting stock {} data for {} days".format(stock, days))
        if not self.__waitAndRequest(xaquery):
            return False

        self.__xaQueries.append(xaquery)
        return True

    def onMessage(self, message, outparam, inparam, sender):
        if message == XAQueryEvents.MSG_DATA_RECEIVED:
            (stock, days, callback, param) = inparam
            xaquery = sender

            datasets = []

            for i in range(0, days):
                val_date            = xaquery.GetFieldData("t1305OutBlock1", "date",       i)
                val_open            = xaquery.GetFieldData("t1305OutBlock1", "open",       i)
                val_high            = xaquery.GetFieldData("t1305OutBlock1", "high",       i)
                val_low             = xaquery.GetFieldData("t1305OutBlock1", "low",        i)
                val_close           = xaquery.GetFieldData("t1305OutBlock1", "close",      i)
                val_sign            = xaquery.GetFieldData("t1305OutBlock1", "sign",       i)
                val_change          = xaquery.GetFieldData("t1305OutBlock1", "change",     i)
                val_diff            = xaquery.GetFieldData("t1305OutBlock1", "diff",       i)
                val_volume          = xaquery.GetFieldData("t1305OutBlock1", "volume",     i)
                val_diff_vol        = xaquery.GetFieldData("t1305OutBlock1", "diff_vol",   i)
                val_chdegree        = xaquery.GetFieldData("t1305OutBlock1", "chdegree",   i)
                val_sojinrate       = xaquery.GetFieldData("t1305OutBlock1", "sojinrate",  i)
                val_changerate      = xaquery.GetFieldData("t1305OutBlock1", "changerate", i)
                val_fpvolume        = xaquery.GetFieldData("t1305OutBlock1", "fpvolume",   i)
                val_covolume        = xaquery.GetFieldData("t1305OutBlock1", "covolume",   i)
                val_shcode          = xaquery.GetFieldData("t1305OutBlock1", "shcode",     i)
                val_value           = xaquery.GetFieldData("t1305OutBlock1", "value",      i)
                val_ppvolume        = xaquery.GetFieldData("t1305OutBlock1", "ppvolume",   i)
                val_o_sign          = xaquery.GetFieldData("t1305OutBlock1", "o_sign",     i)
                val_o_change        = xaquery.GetFieldData("t1305OutBlock1", "o_change",   i)
                val_o_diff          = xaquery.GetFieldData("t1305OutBlock1", "o_diff",     i)
                val_h_sign          = xaquery.GetFieldData("t1305OutBlock1", "h_sign",     i)
                val_h_change        = xaquery.GetFieldData("t1305OutBlock1", "h_change",   i)
                val_h_diff          = xaquery.GetFieldData("t1305OutBlock1", "h_diff",     i)
                val_l_sign          = xaquery.GetFieldData("t1305OutBlock1", "l_sign",     i)
                val_l_change        = xaquery.GetFieldData("t1305OutBlock1", "l_change",   i)
                val_l_diff          = xaquery.GetFieldData("t1305OutBlock1", "l_diff",     i)
                val_marketcap       = xaquery.GetFieldData("t1305OutBlock1", "marketcap",  i)

                dataset = XAData_t1305((val_date, val_open, val_high, val_low, val_close, val_sign, val_change, val_diff, val_volume, val_diff_vol, val_chdegree, val_sojinrate, val_changerate, val_fpvolume, val_covolume, val_shcode, val_value, val_ppvolume, val_o_sign, val_o_change, val_o_diff, val_h_sign, val_h_change, val_h_diff, val_l_sign, val_l_change, val_l_diff, val_marketcap))
                datasets.append(dataset)

            self.__xaQueries.remove(sender)
            self.sendMessage(callback, XADataFetcherDay.MSG_DATA_FETCHED, datasets, param)


class XADatabaseDay(XARunnable):
    BEGINNING = datetime.date(2008, 1, 1)
    DAYS_IN_WEEK = 7.0
    WEEKDAYS_IN_WEEK = 5.0

    def __init__(self, stocks):
        super(XADatabaseDay, self).__init__()
        self.__stocks = stocks
        self.__conn = sqlite3.connect('{}.db'.format(os.path.splitext(sys.argv[0])[0]))
        self.__cur = self.__conn.cursor()
        self.__fetcher = XADataFetcherDay()

    def __del__(self):
        self.__conn.close()

    def _createDatabase(self):
        for stock in self.__stocks:
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
        self.log(Logger.INFO, "XADatabaseDay:updateDatabase called")

        self._createDatabase()

        for stock in self.__stocks:
            last_data = self.__lastData(stock)

            if not last_data:
                last_date = XADatabaseDay.BEGINNING
            else:
                last_date = datetime.datetime.strptime(last_data.date, "%Y%m%d").date()

            days_to_request = int((datetime.date.today() - last_date).days * (XADatabaseDay.WEEKDAYS_IN_WEEK / XADatabaseDay.DAYS_IN_WEEK) + XADatabaseDay.DAYS_IN_WEEK)

            self.log(Logger.INFO, "Updating database - stock({}), last date({}), requesting {} days of data".format(stock, last_date, days_to_request))

            success = self.__fetcher.fetch(stock, days_to_request, self, (stock, days_to_request))

            if not success:
                self.log(Logger.ERROR, "fetch failed")

    def onMessage(self, message, outparam, inparam, sender):
        if message == XADataFetcherDay.MSG_DATA_FETCHED:
            datasets = outparam
            (stock, days_to_request) = inparam

            insert_count = 0
            for dataset in datasets:
                    date = datetime.datetime.strptime(dataset.date, "%Y%m%d").date()

                    if date >= XADatabaseDay.BEGINNING:
                        sqlcommand = "INSERT OR IGNORE INTO t1305_{} (date, open, high, low, close, sign, change, diff, volume, diff_vol, chdegree, sojinrate, changerate, fpvolume, covolume, shcode, value, ppvolume, o_sign, o_change, o_diff, h_sign, h_change, h_diff, l_sign, l_change, l_diff, marketcap) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})".format(stock, dataset.date, dataset.open, dataset.high, dataset.low, dataset.close, dataset.sign, dataset.change, dataset.diff, dataset.volume, dataset.diff_vol, dataset.chdegree, dataset.sojinrate, dataset.changerate, dataset.fpvolume, dataset.covolume, dataset.shcode, dataset.value, dataset.ppvolume, dataset.o_sign, dataset.o_change, dataset.o_diff, dataset.h_sign, dataset.h_change, dataset.h_diff, dataset.l_sign, dataset.l_change, dataset.l_diff, dataset.marketcap)
                        self.__cur.execute(sqlcommand)
                        insert_count += 1
            self.log(Logger.INFO, "{} row inserted".format(insert_count))
            self.__conn.commit()

    def __lastData(self, stock):
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

    def initFetch(self, stock, start):
        self.log(Logger.INFO, 'XADatabaseDay:initFetch called')
        date_string = datetime.datetime.strftime(start, "%Y%m%d")
        sqlcommand = "SELECT * FROM t1305_{} WHERE date > {} ORDER BY date ASC".format(stock, date_string)
        return self.__cur.execute(sqlcommand)

    def fetch(self):
        self.log(Logger.INFO, 'XADatabaseDay:fetch called')
        fetched = self.__cur.fetchone()

        if not fetched:
            return None

        return XAData_t1305(fetched)


class MyStrategy(XAStrategyBase):
    def __init__(self, feeder):
        super(MyStrategy, self).__init__(feeder)

    def onLoggedOn(self):
        pass

    def onLoggedOut(self):
        pass

    def onDisconnected(self):
        pass

    def onBar(self, param):
        print(dataset.date)


def main():
    setup_logger()

    (start_date, end_date) = preprocess_options()

    sched = XAScheduler()

    feeder = XADataFeederDay("000150", start_date, end_date)

    strategy = XAStrategyBase(feeder)

    sched.registerRunnable(strategy)

    sched.run()

if __name__ == "__main__":
    main()
