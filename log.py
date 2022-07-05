import logging
from logging.config import dictConfig
import datetime
class Log:
   def __init__(self):
        date = datetime.date.today()
        file=str(date)+'_example.log'
        #print(file)
        logging.basicConfig(
            filename=file,
            level=logging.DEBUG,
            format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            )