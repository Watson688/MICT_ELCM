import pypyodbc
import numpy
import pandas
from datetime import datetime
from datetime import timedelta


class DataGenerator():
    def __init__(self):
        self.connection_string = connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"

    def Generator(self, analysis_date, worktype, window, n = 1):
        # worktype = CM10
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
                events, events_columns = self.select_events(agv, date, window)
                # writing target variable column
                with open("C:\Code\ELCM\TempData\{}".format(worktype + "_" + work_descr + "_" + str(number_of_points)) + '.csv', "w") as f:
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

    def select_events(self, agv, date, window):
        #SELECT * FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = 'AGV538' AND DATE_EVENT < '2018-06-20 12:00:00.0000000' AND DATE_EVENT > '2018-06-15 00:00:00.0000000' ORDER BY DATE_EVENT DESC
        connection_string = self.connection_string
        with pypyodbc.connect(connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            date_end = datetime.strptime(str(date)[:-1], "%Y-%m-%d %H:%M:%S.%f")
            date_start = date_end - timedelta(days = window)
            query1 = "SELECT \
            [RECORD_ID], [DATE_EVENT], TRIM([DEVICE_ID]) AS DEVICE_ID ,TRIM([ITEMNAME]) AS ITEMNAME, TRIM([ITEMVALUE]) AS ITEMVALUE\
            FROM dbo.FMDS_EVENTS_2018 WHERE DEVICE_ID = '{0}' AND DATE_EVENT < '{1}' AND DATE_EVENT > '{2}' ORDER BY DATE_EVENT DESC".format(agv, str(date_end), str(date_start))
            cursor.execute(query1)
            events = cursor.fetchall()
            events_columns = [column[0] for column in cursor.description]
            return events, events_columns

    def select_errors(self, agv, window):
        pass

    
def main():
    # Boot
    print("Start Generating Datasets...")
    S = DataGenerator()
    S.Generator('2017-12-31 00:00:00.0000000', 'CM10', 3) 
    print("Generating finished...")


if __name__ == '__main__':
    main()
