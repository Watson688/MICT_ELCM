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
        # construct training data set based on the error message
        connection_string = self.connection_string
        with pypyodbc.connect(connection_string, autocommit=True) as conn:
            cursor = conn.cursor()
            print("Selecting target variables")
            query1 = "SELECT DEVICE, DATE_OCCURRED  FROM dbo.FMDS_ERRORS WHERE ERROR_MESSAGE = '{0}' AND DATE_OCCURRED > '{1}' ORDER BY DATE_OCCURRED DESC".format(error_message, analysis_date)
            cursor.execute(query1)
            all_target_variables = cursor.fetchall()
            print("{} rows".format(len(all_target_variables)))
        with open("ITEMNAME.csv") as ff:
            item_names = ff.readlines()
            item_names = [x.strip().replace(",", " ") for x in item_names]
        # store all the rows together
        all_rows = []
        # initilze parameters
        query = "SELECT [RECORD_ID], [DATE_EVENT], TRIM([DEVICE_ID]) AS DEVICE_ID ,TRIM([ITEMNAME]) AS ITEMNAME, TRIM([ITEMVALUE]) AS ITEMVALUE \
        FROM dbo.FMDS_EVENTS_2018 WHERE "
        agvs = []
        size = 10
        # loop all target variables
        for i, target in enumerate(all_target_variables):
            agv = target[0]
            date = target[1]
            # construct query for batch selecting
            query = self.construct_query(agv, date, window, windowtype, query, i, size)
            if (i + 1) % size== 0:
                results = {}
                # batch select
                query += " ORDER BY DATE_EVENT DESC "
                print("start batch selecting")
                events, events_columns  = self.select_events(query)
                for e in events:
                    if e[2] not in results:
                        results[e[2]] = {}
                        for n in item_names:
                            # initializing the role, use null to aviod conficting with real 0s
                            results[e[2]][n] = ["None"]
                    results[e[2]][e[3].replace(",", " ")].append(e[4])
                for agv in agvs:
                    temp_row = []
                    for c in item_names:
                        temp_row.append(self.aggregation(results[agv][c]))
                    all_rows.append(",".join(temp_row) + "," + error_message + "\n")
                print("appending {} rows".format(size))
                # Reset parameters
                query = "SELECT [RECORD_ID], [DATE_EVENT], TRIM([DEVICE_ID]) AS DEVICE_ID ,TRIM([ITEMNAME]) AS ITEMNAME, TRIM([ITEMVALUE]) AS ITEMVALUE \
                FROM dbo.FMDS_EVENTS_2018 WHERE "
                agvs = []

        with open(self.output_directory + "{}".format(error_message) + "_" + str(window) + "_" + windowtype + ".csv", "w") as f:
            print("start writing")
            print("total number of rows: {}".format(len(all_rows)))
            # write the headers
            f.write(",".join(item_names) + "," + "target" + "\n")
            # write rows
            for r in all_rows:
                f.write(r)
        print("generating finished")

    def construct_query(self, agv, date, window, windowtype, query, i, size):
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
        query += "(DEVICE_ID = '{0}' AND DATE_EVENT < '{1}' AND DATE_EVENT > '{2}')".format(agv, str(date_end), str(date_start))
        if (i + 1) % size != 0:
            query += " OR "
        print("test")
        return query
    
    def select_events(self, query):
        connection_string = self.connection_string
        with pypyodbc.connect(connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            events = cursor.fetchall()
            events_columns = [column[0] for column in cursor.description]
            return events, events_columns

    def select_errors(self, agv, window):
        pass

    def aggregation(self, l):
        # need more work
        if len(l) == 1:
            return l[0]
        return l[1]

    
def main():
    # Boot
    print("Start Generating training data...")
    print("Start Time: " + str(datetime.now()))
    S = DataGenerator()
    # start date, error message, window size, time delta type
    S.generator_errormessage('2017-12-31 00:00:00.0000000', 'Management System - Direct Stop', 10, "minute") 
    print("End Time: " + str(datetime.now()))
    print("Generating finished...")


if __name__ == '__main__':
    main()
