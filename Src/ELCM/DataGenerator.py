import os
import glob
import pypyodbc
import numpy
import pandas
from datetime import datetime
from datetime import timedelta


class DataGenerator():
    def __init__(self):
        self.connection_string = connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"
        self.output_directory = "C:\Code\ELCM\TempData\\"

    def Generator_worktype(self, analysis_date, worktype, window, n = 1):
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

    def Generator_errormessage(self, analysis_date, error_message, window, windowtype):
        # construct training data set based on the error message
        connection_string = self.connection_string
        with pypyodbc.connect(connection_string, autocommit=True) as conn:
            clustered_events = {}
            cursor = conn.cursor()
            print("Selecting target variables")
            query1 = "SELECT  DEVICE, DATE_OCCURRED  FROM dbo.FMDS_ERRORS WHERE ERROR_MESSAGE = 'Management System - Direct Stop' ORDER BY DATE_OCCURRED DESC"
            cursor.execute(query1)
            all_target_variables = cursor.fetchall()
        print("Removing old files")
        files = glob.glob("C:\Code\ELCM\TempData\*")
        for f in files:
            os.remove(f)
        print("Removed old files")
        with open(self.output_directory + "{}".format(error_message) + "_" + str(window) + "_" + windowtype + ".csv", "w") as f:
            for target in all_target_variables:
                agv = target[0]
                date = target[1]
                print("Selecting events")
                events, events_columns = self.select_events(agv, date, window, windowtype)
                print("{} events in total".format(len(events)))
                # initializing the role, use null to aviod conficting with real 0s
                for e in evnets_columns:
                    clustered_events[e] = ['NULL']
                for e in events:
                    clustered_events[e[3]].append(e[4])
                print("Start writing")
                # write the header
                f.write(",".join(events_columns) + "," + "target" + "\n")
                # write the row
                temp_row = []
                for c in events_columns:
                    # need to modify aggregation function later, now just choose the first non-null element
                    temp_row.append(self.aggregation(clustered_events[c]))
                    temp_row = [x.replace(",", " ") for x in temp_row]
                    f.write(",".join(temp_row) + "," + erorr_message + "\n")
        print("Generating finished")

    def select_events(self, agv, date, window, windowtype):
        #SELECT * FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = 'AGV538' AND DATE_EVENT < '2018-06-20 12:00:00.0000000' AND DATE_EVENT > '2018-06-15 00:00:00.0000000' ORDER BY DATE_EVENT DESC
        connection_string = self.connection_string
        with pypyodbc.connect(connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            date_end = datetime.strptime(str(date)[:-1], "%Y-%m-%d %H:%M:%S.%f")
            if windowtype == "day":
                date_start = date_end - timedelta(days = window)
            if windowtype == "hour":
                date_start = date_end - timedelta(hours = window)
            if windowtype == "minute":
                date_start = date_end - timedelta(minutes = window)
            if windowtype == "second":
                date_start = date_end - timedelta(seconds = window)
            query1 = "SELECT\
            [RECORD_ID], [DATE_EVENT], TRIM([DEVICE_ID]) AS DEVICE_ID ,TRIM([ITEMNAME]) AS ITEMNAME, TRIM([ITEMVALUE]) AS ITEMVALUE\
            FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = '{0}' AND DATE_EVENT < '{1}' AND DATE_EVENT > '{2}' ORDER BY DATE_EVENT DESC".format(agv, str(date_end), str(date_start))
            cursor.execute(query1)
            events = cursor.fetchall()
            events_columns = [column[0] for column in cursor.description]
            return events, events_columns

    def select_errors(self, agv, window):
        pass

    def aggregation(l):
        return l[1]

    
def main():
    # Boot
    print("Start Generating training data...")
    S = DataGenerator()
    S.Generator_errormessage('2017-12-31 00:00:00.0000000', 'Management System - Direct Stop', 3, "hour") 
    print("Generating finished...")


if __name__ == '__main__':
    main()
