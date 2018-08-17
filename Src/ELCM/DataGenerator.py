import os
import glob
import math
import pypyodbc
import collections
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
        self.all_query_parameters = collections.OrderedDict()
        self.all_query_parameters = {"normal":[], "abnormal":[]}
        self.additional_columns = collections.OrderedDict()
        self.additional_columns = {"AVERAGESPEED": None, "STOPS": None, "DISTANCE": None}

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

    def generator_errormessage(self, analysis_start_date, error_message, window_size, window_type, number_of_data):
        all_rows = []
        # 1. get all feature we need
        with open("ITEMNAME.csv") as f:
            column_names = [x.strip() for x in f.readlines()]
        # 2. select all target variables
        with pypyodbc.connect(self.connection_string, autocommit=True) as conn:
            clustered_events = {}
            cursor = conn.cursor()
            query1 = "SELECT TOP({0}) DEVICE, DATE_OCCURRED FROM dbo.FMDS_ERRORS WHERE ERROR_MESSAGE = '{1}' ORDER BY DATE_OCCURRED DESC".format(str(number_of_data), error_message)
            cursor.execute(query1)
            all_target_variables = cursor.fetchall()
            print("selected {} target variables".format(str(len(all_target_variables))))
        # 3. construct queries for abnormal cases
        print("construct queries for abnormal events")
        for target in all_target_variables:
            agv = target[0]
            date = target[1]
            end_date = datetime.strptime(str(date)[:-1], "%Y-%m-%d %H:%M:%S.%f")
            if window_type == "minutes":
                start_date = end_date - timedelta(minutes=window_size)
            if window_type == "hours":
                start_date = end_date - timedelta(hours=window_size)
            if window_type == "days":
                start_date = end_date - timedelta(days=window_size)
            self.all_query_parameters["abnormal"].append([agv, start_date, end_date])
        # 4. constrcut queries for normal cases
        print("construct queries for normal events")
        self.normal_event_detector(analysis_start_date, error_message, all_target_variables, window_size, window_type)
        number_normal_events = len(self.all_query_parameters["normal"])
        number_abnormal_events = len(self.all_query_parameters["abnormal"])
        print("total abnormal events: {0}, total normal events: {1}".format(number_abnormal_events, number_normal_events))
        # 5. start selecting events, and clean the selected data
        count = 1
        print("selecting events")
        for k, v in self.all_query_parameters.items():
            for parameters in v:
                agv = parameters[0]
                start_date = parameters[1]
                end_date = parameters[2]
                # select events
                events = self.select_events(agv, start_date, end_date)
                print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "  {} events in the row".format(len(events)) + " , " +  "{0} / {1}".format(str(count), str(number_abnormal_events+number_normal_events)))
                # initializing the role, use null to aviod conficting with real 0s
                for c in column_names:
                    clustered_events[c] = ['None']
                for e in events:
                    feature = e[3]
                    if feature in column_names:
                        clustered_events[feature].append(e[4])
                temp_row = []
                for c in column_names:
                    # aggregation and back-tracing
                    element = self.aggregation(c, clustered_events[c], agv, start_date)
                    temp_row.append(element)
                additional = ",".join([str(ex) for ex in list(self.additional_columns.values())])
                self.reset()
                if k =="abnormal":
                    all_rows.append(",".join([str(x) for x in temp_row]) + "," + additional + "," + "1" + "\n")
                if k == "normal":
                    all_rows.append(",".join([str(x) for x in temp_row]) + "," + additional + "," + "0" + "\n")
                count += 1
        file_name = self.output_directory + "{}".format(error_message) + "_" + str(window_size) + "_" + window_type + ".csv"
        # 6. write to the file
        with open(file_name, "w") as f:
            # write the header
            column_names = [x.replace(",", " ") for x in column_names]
            f.write(",".join(column_names) + "," + ",".join(list(self.additional_columns))+ "," + "target" + "\n")
            # write the rows
            for r in all_rows:
                f.write(r)

    def normal_event_detector(self, analysis_start_date, error_message, all_target_variable, window_size, window_type):
        """ use to generate normal case, part of the generator_errormessage
        """
        for target in all_target_variable:
            agv = target[0]
            # 1. select all dates that the error_message happened for that agv, want to know the time gap between each error, so we can find out the time window that the avg works normally
            with pypyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                query = "SELECT DATE_OCCURRED FROM FMDS_ERRORS WHERE DEVICE = '{0}' AND ERROR_MESSAGE = '{1}' AND DATE_OCCURRED BETWEEN '{2}' AND '{3}' ORDER BY DATE_OCCURRED DESC".format(agv, error_message, analysis_start_date, target[1])
                cursor.execute(query)
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
                        if window_type == "minutes":
                            size = timedelta(minutes=window_size)
                        if window_type == "hours":
                            size = timedelta(hours=window_size)
                        if window_type == "days":
                            size = timedelta(days=window_size)
                        if delta_time >= 2 * size:
                            start_date = t2
                            end_date = t2 + size
                            self.all_query_parameters["normal"].append([agv, start_date, end_date])
                            break
                    
    def select_events(self, agv, start_date, end_date):
        with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            #SELECT * FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = 'AGV538' AND DATE_EVENT < '2018-06-20 12:00:00.0000000' AND DATE_EVENT > '2018-06-15 00:00:00.0000000' ORDER BY DATE_EVENT DESC
            query1 = "SELECT\
            [RECORD_ID], [DATE_EVENT], TRIM([DEVICE_ID]) AS DEVICE_ID ,TRIM([ITEMNAME]) AS ITEMNAME, TRIM([ITEMVALUE]) AS ITEMVALUE\
            FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = '{0}' AND DATE_EVENT between '{1}' AND '{2}' ORDER BY DATE_EVENT DESC".format(agv, str(start_date), str(end_date))
            cursor.execute(query1)
            events = cursor.fetchall()
        return events

    def aggregation(self, event_type, series, agv, traceback_end_date):
        start_date = traceback_end_date - timedelta(days=self.trace_max)
        if event_type == "PositionX,PositionY,Velocity,Arc":
            if len(series) > 2:
                distance = 0
                stop = 0
                speed = 0
                for i, p in enumerate(series):
                    # start from the 3rd elements
                    if i < 2:
                        continue
                    p = p.split(",")
                    x_now = p[0]
                    y_now = p[1]
                    x_pre = series[i-1].split(",")[0]
                    y_pre = series[i-1].split(",")[1]
                    if float(p[2]) == 0:
                        stop += 1
                    speed += float(p[2])
                    # Euclidean distance
                    distance += self.calculate_distance(x_now, y_now, x_pre, y_pre)
                # for straight-line distance
                temp_last = series[-1].split(",")
                temp_first = series[1].split(",")
                x_now = temp_last[0]
                y_now = temp_last[1]
                x_pre = temp_first[0]
                y_pre = temp_last[1]
                self.additional_columns["DISTANCE"] = self.calculate_distance(x_now, y_now, x_pre, y_pre)
                self.additional_columns["AVERAGESPEED"] = speed / (len(series) - 1)
                self.additional_columns["STOPS"] = stop
                return distance
            else:
                # only 1 position data, which means didn't move
                return 0
        elif len(series) == 1:
            # back trace
            try:
                with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
                    cursor = conn.cursor()  
                    query = "SELECT TOP(1) TRIM(ITEMVALUE) AS ITEMVALUE FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = '{0}' AND DATE_EVENT < '{1}' AND DATE_EVENT > '{2}' AND ITEMNAME = '{3}' ORDER BY DATE_EVENT DESC".format(agv, traceback_end_date, start_date, event_type)
                    cursor.execute(query)
                    r = cursor.fetchall()[0][0]
            except Exception as e:
                return 'None'
            return r
        # calculate distance traveled
        else:
            return np.mean([int(x) for x in series[1:]])
    
    def calculate_distance(self, x_1, y_1, x_2, y_2):
        return math.sqrt( (float(x_1) - float(x_2))**2 + (float(y_1) - float(y_2))**2 )

    def reset(self):
        self.additional_columns = {"AVERAGESPEED": None, "STOPS": None, "DISTANCE": None}

def main():
    # Boot
    print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "  start generating training data")
    S = DataGenerator()
    # start date, error message, window size, time delta type
    S.generator_errormessage('2017-12-31 00:00:00.0000000', 'Management System - Direct Stop', 4, "hours", 1000) 
    print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "  generating finished")


if __name__ == '__main__':
    main()
     