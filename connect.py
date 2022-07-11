import logging
import pyodbc
#""" The Conenct class
# input user and pwd
# output : cnxn object once connected to db
#"""

class Connect:
    def __init__(self):
        self.user = "kskarzynski"
        self.pwd = "zar519chr!"
    def connectdb(self):
        try:
            self.cnxn = pyodbc.connect(
                "DRIVER={SQL Server};SERVER=10.2.115.129;DATABASE=devdb;UID=" + (self.user) + ";PWD=" + (self.pwd))
        except Exception as e:
            logging.error(e)
            exit
        return self.cnxn
