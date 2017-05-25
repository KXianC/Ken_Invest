import pandas as pd
import os
import time
from datetime import datetime

path = "/Users/Ken/Documents/Python_MachineLearning_for_Investing/intraQuarter"

def Key_Stats(gather="Total Debt/Equity (mrq)"):
    statspath = path + '/_KeyStats'
    stock_list = [x[0] for x in os.walk(statspath)]
    #print(stock_list)
    for each_dir in stock_list[1:]:
        each_file = os. listdir(each_dir)
        ticker = each_dir.split("/")[-1]
        #print(eachfile)
        if len(each_file) > 0:
            for file in each_file:
                date_stamp = datetime.strptime(file, '%Y%m%d%H%M%S.html')
                unix_time = time.mktime(date_stamp.timetuple())
                #print(date_stamp, unix_time)
                full_file_path = each_dir+'/'+file
                #print(full_file_path)
                #time.sleep(10)
                source = open(full_file_path,'r').read()
                #print(source)
                value = source.split(gather+':</td><td class="yfnc_tabledata1">')[1].split('</td></tr><tr>')[0]
                print(ticker+":",value)




            time.sleep(10)


Key_Stats()



#0.407
