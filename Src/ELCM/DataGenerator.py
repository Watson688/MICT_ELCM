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
        self.output_directory = "C:\Code\MICT_ELCM\TempData\\"
        # maximum trace back days
        self.trace_max = 3
        self.all_query_parameters = collections.OrderedDict()
        self.all_query_parameters = {"normal":[], "abnormal":[]}
        self.additional_columns = collections.OrderedDict()
        self.additional_columns = {"AVERAGESPEED": None, "STOPS": None, "DISTANCE": None, "D_To_TPX": None, "T_To_TPX": None}

    # This function is not in use now
    def generator_worktype(self, analysis_date, worktype, window, n = 1):
        # construct training data set based on the worktype
        # select target work description
        with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
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

    def generator_errormessage(self, analysis_start_date, error_message, window_size, window_type, normal_case_type, number_of_data=None):
        all_rows = []
        # 1. get all features we need
        with open("ITEMNAME.csv") as f:
            column_names = [x.strip() for x in f.readlines()]
        # 2. select all target variables
        with pypyodbc.connect(self.connection_string, autocommit=True) as conn:
            clustered_events = {}
            cursor = conn.cursor()
            if number_of_data is not None:
                query1 = "SELECT TOP({0}) DEVICE, DATE_OCCURRED, POSITION FROM dbo.FMDS_ERRORS WHERE ERROR_MESSAGE = '{1}' AND (OPERATIONAL_MODE = 'Activated' OR OPERATIONAL_MODE = 'Allocated' OR OPERATIONAL_MODE = 'Driving') ORDER BY DATE_OCCURRED DESC".format(number_of_data, error_message)
            else:
                query1 = "SELECT DEVICE, DATE_OCCURRED, POSITION FROM dbo.FMDS_ERRORS WHERE ERROR_MESSAGE = '{0}' AND DATE_OCCURRED > '{1}' AND (OPERATIONAL_MODE = 'Activated' OR OPERATIONAL_MODE = 'Allocated' OR OPERATIONAL_MODE = 'Driving') ORDER BY DATE_OCCURRED DESC".format(error_message, analysis_start_date)
            cursor.execute(query1)
            all_target_variables = cursor.fetchall()
            print("selected {} target variables".format(str(len(all_target_variables))))
        # 3. construct queries for abnormal cases
        print("construct queries for abnormal events")
        for target in all_target_variables:
            agv = target[0]
            date = target[1]
            t_position = target[2].split(",")
            end_date = datetime.strptime(str(date)[:-1], "%Y-%m-%d %H:%M:%S.%f")
            if window_type == "minutes":
                start_date = end_date - timedelta(minutes=window_size)
            if window_type == "hours":
                start_date = end_date - timedelta(hours=window_size)
            if window_type == "days":
                start_date = end_date - timedelta(days=window_size)
            self.all_query_parameters["abnormal"].append([agv, start_date, end_date, t_position])
        # 4. constrcut queries for normal cases
        print("construct queries for normal events")
        if normal_case_type == 1:
            self.normal_event_detector_1(analysis_start_date, error_message, all_target_variables, window_size, window_type)
        if normal_case_type == 2:
            self.normal_event_detector_2(analysis_start_date, error_message, self.all_query_parameters["abnormal"], window_size, window_type)
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
                position = parameters[3]
                # select events
                events = self.select_events(agv, start_date, end_date)
                print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "  {} events in the row".format(len(events)) + " , " +  "{0} / {1}".format(str(count), str(number_abnormal_events+number_normal_events)))
                # initializing the role, use null to aviod conficting with real 0s
                for c in column_names:
                    clustered_events[c] = ['None']
                for e in events:
                    feature = e[3]
                    if feature in column_names:
                        # special case for this feature, because we also need the timestamp in e[1]
                        if feature == "DefectTPX,DefectTPY,DefectTPDate,DefectTPTime,DefectTPAntennaPos":
                            clustered_events[feature].append(e[4] + "," + e[1])
                        else:
                            clustered_events[feature].append(e[4])
                temp_row = []
                for c in column_names:
                    # aggregation and back-tracing
                    element = self.aggregation(c, clustered_events[c], agv, start_date, end_date, position)
                    temp_row.append(element)
                additional = ",".join([str(ex) for ex in list(self.additional_columns.values())])
                self.reset()
                if k =="abnormal":
                    all_rows.append(",".join([str(x) for x in temp_row]) + "," + additional + "," + "1" + "\n")
                if k == "normal":
                    all_rows.append(",".join([str(x) for x in temp_row]) + "," + additional + "," + "0" + "\n")
                count += 1
        file_name = self.output_directory + "{}".format(error_message) + "_" + str(window_size) + "_" + window_type + "_" + str(2*len(all_target_variables)) + ".csv"
        # 6. write to the file
        with open(file_name, "w") as f:
            # cleanup the header
            column_names = self.clean_column_names(column_names)
            # write the header
            column_names = [x.replace(",", " ") for x in column_names]
            f.write(",".join(column_names) + "," + ",".join(list(self.additional_columns))+ "," + "target" + "\n")
            # write the rows
            for r in all_rows:
                f.write(r)

    def normal_event_detector_1(self, analysis_start_date, error_message, all_target_variable, window_size, window_type):
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
                    for i, d in enumerate(all_dates[1:], 1):
                        t1 = datetime.strptime(str(all_dates[i-1])[:-1], "%Y-%m-%d %H:%M:%S.%f")
                        t2 = datetime.strptime(str(d)[:-1], "%Y-%m-%d %H:%M:%S.%f")
                        delta_time = t1 - t2
                        # how large the delta time we need depends on the window size, now we gonna check it
                        # we want the delta_time is at least two times larger than the window size
                        if window_type == "minutes":
                            size = timedelta(minutes=window_size)
                        if window_type == "hours":
                            size = timedelta(hours=window_size)
                        if window_type == "days":
                            size = timedelta(days=window_size)
                        if delta_time >= 2 * size:
                            start_date = t2
                            end_date = t2 + size
                            self.all_query_parameters["normal"].append([agv, start_date, end_date, target[2].split(",")])
                            break

    def normal_event_detector_2(self, analysis_start_date, error_message, query_list, window_size, window_type):
        # choose the agv with maximum events
        for target in query_list:
            agv = target[0]
            start_date = target[1]
            end_date = target[2]
            if window_type == "minutes":
                size = timedelta(minutes=window_size)
            if window_type == "hours":
                size = timedelta(hours=window_size)
            if window_type == "days":
                size = timedelta(days=window_size)
            end_date_ = end_date + size
            with pypyodbc.connect(self.connection_string) as conn:
                cursor = conn.cursor()
                query1 = "SELECT * FROM dbo.FMDS_ERRORS \
                WHERE DATE_OCCURRED between '{0}' AND '{1}' AND ERROR_MESSAGE = '{2}' AND DEVICE != '{3}'".format(start_date, end_date_, error_message, agv)
                cursor.execute(query1)
                all_errors = cursor.fetchall()
                black_list = [x[1] for x in all_errors if x[1] != agv]
                agv_events = {}
                max_ = [0, None]
                query2 = "SELECT TRIM(DEVICE_ID) AS DEVICE_ID, COUNT(DEVICE_ID) AS TOTAL_ FROM dbo.FMDS_EVENTS_2018 WHERE DATE_EVENT between '{0}' AND '{1}' GROUP BY DEVICE_ID ORDER BY TOTAL_ DESC".format(start_date, end_date)
                cursor.execute(query2)
                all_agvs =cursor.fetchall()
                for aa in all_agvs:
                    if aa[0] not in black_list:
                        # get the position for the agv
                        query3 = "SELECT TOP(1) TRIM(ITEMVALUE) FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = '{2}' AND ITEMNAME = 'PositionX,PositionY,Velocity,Arc' AND DATE_EVENT BETWEEN '{0}' AND '{1}' ORDER BY DATE_EVENT DESC".format(start_date, end_date, aa[0])
                        cursor.execute(query3)
                        position = cursor.fetchall()[0][0].split(",")
                        self.all_query_parameters["normal"].append([agv, start_date, end_date, position])
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

    def aggregation(self, event_type, series, agv, traceback_end_date, end_date, position):
        """ This is the aggregation function used for aggregate a event time series to one single data
        """
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
        elif event_type == "DefectTPX,DefectTPY,DefectTPDate,DefectTPTime,DefectTPAntennaPos":
            if len(series) > 1:
                aver_tpx_err = []
                for s in series[1:]:
                    ss = s.split(",")
                    tt = ss[2] + "." + ss[3]
                    aver_tpx_err.append(datetime.strptime(tt, "%d.%m.%Y.%H:%M:%S.%f"))
                test_ = list(zip(aver_tpx_err[:-1], aver_tpx_err[1:]))
                aver_tpx_err = [(i-j).total_seconds() for i, j in zip(aver_tpx_err[:-1], aver_tpx_err[1:])]
                # D_To_TPX
                last_itemvalue = series[1].split(",")
                last_position_x = last_itemvalue[0]
                last_position_y = last_itemvalue[1]
                # T_To_TPX
                last_time = datetime.strptime(last_itemvalue[2] + "." + last_itemvalue[3], "%d.%m.%Y.%H:%M:%S.%f")
                if position[0] != 'n/a':
                    self.additional_columns["D_To_TPX"] = self.calculate_distance(last_position_x, last_position_y, position[0], position[1])
                else:
                    self.additional_columns["D_To_TPX"] = None
                self.additional_columns["T_To_TPX"] = (end_date - datetime.strptime(last_itemvalue[5][:-1], "%Y-%m-%d %H:%M:%S.%f")).total_seconds()
                return np.mean(aver_tpx_err)
            else:
                return -1
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
        # all other event type that length longer that 1
        else:
            return np.mean([int(x) for x in series[1:]])
    
    def calculate_distance(self, x_1, y_1, x_2, y_2):
        return math.sqrt( (float(x_1) - float(x_2))**2 + (float(y_1) - float(y_2))**2 )

    def reset(self):
        for k, v in self.additional_columns.items():
            self.additional_columns[k] = None

    def clean_column_names(self, header):
        for i, name in enumerate(header):
            if name == "DefectTPX,DefectTPY,DefectTPDate,DefectTPTime,DefectTPAntennaPos":
                header[i] = "AVERAGE TPX ERROR GAP"
        return header

    def minute_interval_events(self, start_date, interval_size):
        output = []
        start_date = datetime.strptime(start_date[:-1], "%Y-%m-%d %H:%M:%S.%f")
        # temporary end date
        end_date = datetime.strptime('2018-06-28 00:00:00.000000', "%Y-%m-%d %H:%M:%S.%f")
        """
            with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
                cursor = conn.cursor()
                query1 = "SELECT TOP(1) DATE_EVENT FROM [MICT_ELCM].[dbo].[FMDS_EVENTS_2018] ORDER BY DATE_EVENT DESC"
                cursor.execute(query1)
                end_date = datetime.strptime(cursor.fetchall()[0][0][:-1], "%Y-%m-%d %H:%M:%S.%f")
        """
        with open("AGVNAMES.csv") as f:
            AGV = [x.strip() for x in f.readlines()]
        with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            interval_start = start_date
            interval_end = start_date + timedelta(minutes=interval_size)
            while interval_end <= end_date:
                print("current interval: {0} to {1}".format(interval_start, interval_end))
                for index, agv in enumerate(AGV):
                    query = "SELECT DATE_EVENT, TRIM(DEVICE_ID) AS DEVICE_ID, TRIM(ITEMNAME) AS ITEMNAME, TRIM(ITEMVALUE) AS ITEMVALUE FROM [MICT_ELCM].[dbo].[FMDS_EVENTS_2018] WHERE DEVICE_ID = '{0}' \
                    AND ITEMNAME = 'PositionX,PositionY,Velocity,Arc' AND DATE_EVENT BETWEEN '{1}' AND '{2}' ORDER BY DATE_EVENT".format(agv, interval_start, interval_end)
                    cursor.execute(query)
                    events = cursor.fetchall()
                    if events:
                        if len(events) == 1:
                            if events[0][-1].split(',')[-2] == '0':
                                output.append(str(interval_start) + ',' + str(agv) + ',' + '0' + ',' + '1')
                            else:
                                output.append(str(interval_start) + ',' + str(agv) + ',' + '0' + ',' + '0')
                        else:
                            distance = 0
                            coordinates = []
                            stops = 0
                            for row in events:
                                itemvalues = row[-1].split(",")
                                coordinates.append((itemvalues[0], itemvalues[1]))
                                if itemvalues[-2] == 0:
                                    stops += 1
                            for i, c in enumerate(coordinates):
                                if i == 0:
                                    continue
                                else:
                                    distance += self.calculate_distance(coordinates[i-1][0], coordinates[i-1][1], c[0], c[1])
                            output.append(str(interval_start) + ',' + str(agv) + ',' + str(distance) + ',' + str(stops))
                interval_start = interval_end + timedelta(seconds=1)
                interval_end = interval_end + timedelta(minutes=interval_size)
        with open(self.output_directory + str(start_date.date()) + "_" + str(end_date.date()) + "_" + str(interval_size) + ".csv", "w") as f:
            f.write("date,device,distance,stops\n")
            for o in output:
                f.write(o + '\n')



def main():
    # Boot
    print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "  start generating training data")
    S = DataGenerator()
    # start date, error message, window size, window size type, number of data
    # number of data can be a int or None
    # if None, the program will return all the errors after the start date, for example: 2018-01-01 00:00:00.0000000
    # int means the total failed cases you want
    #S.generator_errormessage('2018-01-01 00:00:00.0000000', 'Management System - Direct Stop', 4, "hours", 1, 10) 
    S.minute_interval_events('2018-01-01 00:00:00.0000000', 120)
    print(str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "  generating finished")


if __name__ == '__main__':
    main()
     