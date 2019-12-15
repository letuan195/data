import time
import schedule
import daily_data

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


class SMWinservice(win32serviceutil.ServiceFramework):
    '''Base class to create winservice in Python'''
    _svc_name_ = 'xxxService'
    _svc_display_name_ = 'Python Service'
    _svc_description_ = 'Python Service Description'

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
        self.isrunning = True

    def stop(self):
        '''
        Override to add logic before the stop
        eg. invalidating running condition
        '''
        self.isrunning = False


    def main(self):
        i = 0
        while self.isrunning and i < 10:
            daily_data.test_run()
            i = i + 1
            time.sleep(1)
        # crawl_historical_data.run()
        # schedule.every().day.at("17:00").do(crawl_historical_data.run)
        # while True:
        #     schedule.run_pending()
        #     time.sleep(1)



if __name__ == '__main__':
    SMWinservice.parse_command_line()

