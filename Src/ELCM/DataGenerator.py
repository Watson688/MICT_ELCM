import os
import glob
import pypyodbc
import numpy
import pandas
from datetime import datetime
from datetime import timedelta


class DataGenerator():
    def __init__(self):
        self.connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"
        self.output_directory = "C:\Code\ELCM\TempData\\"

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

    def generator_errormessage(self, analysis_date, error_message, window, windowtype):
        with open("ITEMNAME.csv") as f:
            column_names = [ x.strip() for x in f.readlines()]
        # construct training data set based on the error message
        all_rows = []
        with pypyodbc.connect(self.connection_string, autocommit=True) as conn:
            clustered_events = {}
            cursor = conn.cursor()
            print("selecting target variables")
            query1 = "SELECT TOP(5) DEVICE, DATE_OCCURRED  FROM dbo.FMDS_ERRORS WHERE ERROR_MESSAGE = 'Management System - Direct Stop' ORDER BY DATE_OCCURRED DESC"
            cursor.execute(query1)
            all_target_variables = cursor.fetchall()
        count = 1
        print("selecting events")
        for target in all_target_variables:
            agv = target[0]
            date = target[1]
            # select events
            events, trace_back_date = self.select_events(agv, date, window, windowtype, cursor)
            print( "{} events in the row".format(len(events)) + " , " +  "{0} / {1}".format(str(count), str(len(all_target_variables))) )
            # initializing the role, use null to aviod conficting with real 0s
            for c in column_names:
                clustered_events[c] = ['None']
            for e in events:
                if e[3] == "PositionX,PositionY,Velocity,Arc" or e[3] == "DefectTPX,DefectTPY,DefectTPDate,DefectTPTime,DefectTPAntennaPos"\
                    or e[3] == "Batt1StateOfCharge,Batt2StateOfCharge,Batt3StateOfCharge,Batt4StateOfCharge,Batt5StateOfCharge,Batt6StateOfCharge,Batt7StateOfCharge,Batt8StateOfCharge,Batt9StateOfCharge" \
                    or e[3] == "PLCBrakeResistorFrontTemperature"\
                    or e[3] == "PLCBrakeResistorRearTemperature":
                    continue
                else:
                    clustered_events[e[3]].append(e[4])
            temp_row = []
            for c in column_names:
                # need to modify aggregation function later, now just choose the first non-null element
                temp_row.append(self.aggregation(c, clustered_events[c], agv, trace_back_date))
            all_rows.append(",".join([x.replace(",", " ") for x in temp_row]) + "," + error_message + "\n")
            count += 1
        with open(self.output_directory + "{}".format(error_message) + "_" + str(window) + "_" + windowtype + ".csv", "w") as f:
            # write the header
            f.write(",".join(column_names) + "," + "target" + "\n")
            for r in all_rows:
                f.write(r)

    def select_events(self, agv, date, window, windowtype, cursor):
        with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            #SELECT * FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = 'AGV538' AND DATE_EVENT < '2018-06-20 12:00:00.0000000' AND DATE_EVENT > '2018-06-15 00:00:00.0000000' ORDER BY DATE_EVENT DESC
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
        return events, date_start

    def select_errors(self, agv, window):
        pass

    def aggregation(self, event_type, l, agv, end_date):
        # need more work 
        if len(l) == 1:
            # back trace
            with pypyodbc.connect(self.connection_string, autocommit = True) as conn:
                cursor = conn.cursor()
                query = "SELECT TOP(1) TRIM(ITEMVALUE) AS ITEMVALUE FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = '{0}' AND DATE_EVENT < '{1}' AND ITEMNAME = '{2}' ORDER BY DATE_EVENT DESC".format(agv, end_date, event_type)
                cursor.execute(query)
                r = cursor.fetchall()
            return r[0][0]
        else:
            return l[1]

    
def main():
    # Boot

    print("start Generating training data...")
    print(str(datetime.now()))
    S = DataGenerator()
    # start date, error message, window size, time delta type
    S.generator_errormessage('2017-12-31 00:00:00.0000000', 'Management System - Direct Stop', 20, "minute") 
    print(str(datetime.now()))
    print("generating finished...")


if __name__ == '__main__':
    main()
