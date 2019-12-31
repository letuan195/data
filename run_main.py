import time
import datetime
import schedule
import main

import socket
import win32serviceutil
import servicemanager
import win32event
import win32service

import sys
import os
BASER_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(BASER_DIR)
DATABASE_DIR = os.path.join(BASER_DIR, "databases")
sys.path.append(DATABASE_DIR)

from databases.db_log import error


class SMWinservice(win32serviceutil.ServiceFramework):
    '''Base class to create winservice in Python'''
    _svc_name_ = 'pyAlgo'
    _svc_display_name_ = 'Python Data Algo Service'
    _svc_description_ = 'Get data from fireant by python'

    @classmethod
    def parse_command_line(cls):
        '''
        ClassMethod to parse the command line
        '''
        win32serviceutil.HandleCommandLine(cls)

    def __init__(self, args):
        '''
        Constructor of the winservice
        '''
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        socket.setdefaulttimeout(60)

    def SvcStop(self):
        '''
        Called when the service is asked to stop
        '''
        self.stop()
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        '''
        Called when the service is asked to start
        '''
        self.start()
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def start(self):
        '''
        Override to add logic before the start
        eg. running condition
        '''

    def stop(self):
        '''
        Override to add logic before the stop
        eg. invalidating running condition
        '''
        error("Service is being stopped at: {0}".format(datetime.datetime.now()))

    def main(self):
        # daily_data.run()
        schedule.every().day.at("17:00").do(main.run)
        while True:
            schedule.run_pending()
            time.sleep(1)



if __name__ == '__main__':
    SMWinservice.parse_command_line()

