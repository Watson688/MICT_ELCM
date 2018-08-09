import os
import glob
import pypyodbc
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta


class DataGenerator():
    def __init__(self):
        self.connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"
        self.output_directory = "C:\Code\ELCM\TempData\\"
        # maximum trace back days
        self.trace_max = 3
    
    # not in use now
    def generator_worktype(self, analysis_date, worktype, window, n = 1):
        # construct training data set based on the worktype
        connection_string = self.connection_string
        # select target work description
        with pypyodbc.connect(connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            # select target work description
            query1 = "SELECT TOP({}) COUNT(WO_DESCR) AS TOTAL, WO_DESCR, MAX(WO_WORKTYPE) AS WO_WORKTYPE FROM \
            (SELECT * FROM dbo.MAXIMO_WORKORDERS WHERE WO_WORKTYPE = '{}') A GROUP BY WO_DESCR ORDER BY TOTAL DESC".format(str(n), worktype)
            cursor.execute(query1)
            columns = [column[0] for column in cursor.description]
            work_descr = cursor.fetchall()[0][1]
            # select all work_order for the target work description
            query2 = "SELECT * \
            FROM dbo.MAXIMO_WORKORDERS WHERE WO_WORKTYPE = '{0}' AND WO_DESCR = '{1}' AND WO_REPORTDATE > '{2}' ORDER BY WO_REPORTDATE DESC".format(worktype, work_descr, analysis_date)
            cursor.execute(query2)
            all_target_variables = cursor.fetchall()
            print("{} points found".format(str(len(all_target_variables))))
            target_variables_columns = [column[0] for column in cursor.description]
        number_of_points = 0
        for r in all_target_variables:
            agv = r[6][:-1]
            date = r[-2]
            # select all events
            events, events_columns = self.select_events(agv, date, window, "day")
            # writing target variable column
            with open(self.output_directory + "{}".format(worktype + "_" + work_descr + "_" + str(number_of_points)) + '.csv', "w") as f:
                f.write(",".join(target_variables_columns) + "\n") 
                # writing target variable
                f.write(",".join([str(rr) for rr in r]) + "\n")
                # writing event column
                f.write(",".join(events_columns) + "\n")
                for row in events:
                    f.write(",".join([str(rr) for rr in row]) + "\n")
            number_of_points += 1
            print("wrote {} files".format(str(number_of_points)))
        print("writing finished")

    def generator_errormessage(self, analysis_start_date, error_message, window_size, windowtype):
        with open("ITEMNAME.csv") as f:
            column_names = [ x.strip() for x in f.readlines()]
        # construct training data set based on the error message
        all_rows = []
        with pypyodbc.connect(self.connection_string, autocommit=True) as conn:
            clustered_events = {}
            cursor = conn.cursor()
            print("selecting target variables")
            query1 = "SELECT TOP(1000) DEVICE, DATE_OCCURRED FROM dbo.FMDS_ERRORS WHERE ERROR_MESSAGE = '{0}' ORDER BY DATE_OCCURRED DESC".format(error_message)
            cursor.execute(query1)
            all_target_variables = cursor.fetchall()
            print("selected {} target variables".format(str(len(all_target_variables))))
        count = 1
        print("selecting events")
        for target in all_target_variables:
            agv = target[0]
            date = target[1]
            # select events
            events, trace_back_date = self.select_events(agv, date, window_size, windowtype)
            print( "{} events in the row".format(len(events)) + " , " +  "{0} / {1}".format(str(count), str(len(all_target_variables))) )
            # initializing the role, use null to aviod conficting with real 0s
            for c in column_names:
                clustered_events[c] = ['None']
            for e in events:
                if e[3] in column_names:
                    clustered_events[e[3]].append(e[4])
            temp_row = []
            for c in column_names:
                # need to modify aggregation function later, now just choose the first non-null element
                temp_row.append(self.aggregation(c, clustered_events[c], agv, trace_back_date))
            all_rows.append(",".join([str(x) for x in temp_row]) + "," + error_message + "\n")
            count += 1
        file_name = self.output_directory + "{}".format(error_message) + "_" + str(window_size) + "_" + windowtype + ".csv"
        with open(file_name, "w") as f:
            # write the header
            f.write(",".join(column_names) + "," + "target" + "\n")
            for r in all_rows:
                f.write(r)
        # write down the normal case without error to the same file
        self.normal_event_detector(analysis_start_date, error_message, all_target_variable, file_name, window_size)

    def normal_event_detector(self, analysis_start_date, error_message, all_target_variable, file, window_size, window_type):
        """ use to generate normal case, part of the generator_errormessage
        """
        for target in all_target_variable:
            # 1. select all dates that the error_message happened for that agv, want to know the time gap between each error, so we can find out the time window that the avg works normally
            with pypyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                query = "SELECT DATE_OCCURRED FROM FMDS_ERRORS WHERE DEVICE = '{0}' AND ERROR_MESSAGE = '{1}' AND DATE_OCCURRED BETWEEN '{3}' AND {4} ORDER BY DATE_OCCURRED DESC".format(target[0], error_message, analysis_start_date, target[1])
                cursor.execute()
                all_dates = cursor.fetchall()
                if len(all_dates) != 0:
                    all_dates = [d[0] for d in all_dates]
                    # calculate the delta time between each error
                    for i, d in enumerate(all_dates[1:]):
                        t1 = datetime.strptime(str(all_dates[i-1])[:-1], "%Y-%m-%d %H:%M:%S.%f")
                        t2 = datetime.strptime(str(d)[:-1], "%Y-%m-%d %H:%M:%S.%f")
                        delta_time = t2 - t1
                        # how large the delta time we need depends on the window size, now we gonna check it
                        # we want the delta_time is at least two time larger than the window size
                        exec("size = timedelta({0} = {1})".format(window_type, window_size))
                        if delta_time >= 2 * size:
                            start_date = t2
                            end_date = t2 + size
                            break






    def select_events(self, agv, date, window, windowtype):
        with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            #SELECT * FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = 'AGV538' AND DATE_EVENT < '2018-06-20 12:00:00.0000000' AND DATE_EVENT > '2018-06-15 00:00:00.0000000' ORDER BY DATE_EVENT DESC
            date_end = datetime.strptime(str(date)[:-1], "%Y-%m-%d %H:%M:%S.%f")
            exec("date_start = date_end - timedelta({0} = {1})".format(windowtype, window))
            query1 = "SELECT\
            [RECORD_ID], [DATE_EVENT], TRIM([DEVICE_ID]) AS DEVICE_ID ,TRIM([ITEMNAME]) AS ITEMNAME, TRIM([ITEMVALUE]) AS ITEMVALUE\
            FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = '{0}' AND DATE_EVENT < '{1}' AND DATE_EVENT > '{2}' ORDER BY DATE_EVENT DESC".format(agv, str(date_end), str(date_start))
            cursor.execute(query1)
            events = cursor.fetchall()
        events_columns = [column[0] for column in cursor.description]
        return events, date_start

    def aggregation(self, event_type, l, agv, end_date):
        start_date = end_date - timedelta(days=self.trace_max)
        # need more work 
        if len(l) == 1:
            # back trace
            try:
                with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
                    cursor = conn.cursor()  
                    query = "SELECT TOP(1) TRIM(ITEMVALUE) AS ITEMVALUE FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = '{0}' AND DATE_EVENT < '{1}' AND DATE_EVENT > '{2}' AND ITEMNAME = '{3}' ORDER BY DATE_EVENT DESC".format(agv, end_date, start_date, event_type)
                    cursor.execute(query)
                    r = cursor.fetchall()[0][0]
            except Exception as e:
                return 'None'
            return r
        else:
            return np.mean([int(x) for x in l[1:]])

def main():
    # Boot

    print("start Generating training data...")
    print(str(datetime.now()))
    S = DataGenerator()
    # start date, error message, window size, time delta type
    S.generator_errormessage('2017-12-31 00:00:00.0000000', 'Management System - Direct Stop', 2, "hours") 
    print(str(datetime.now()))
    print("generating finished...")


if __name__ == '__main__':
    main()
