import pypyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class TempDict(object):
    def __init__(self, ErrorMessage):
        self.dict = {}
        for m in ErrorMessage:
            self.dict[m[1].strip()] = 0


class EAD(object):
    def __init__(self):
        pass

    def DailyError(self):
        connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"
        with pypyodbc.connect(connection_string, autocommit = True) as conn:
            # Retrieve data from database
            print("Retrieving data from database")
            cursor = conn.cursor()
            q1 = "SELECT ERROR_TYPE, ERROR_MESSAGE, CONVERT(DATE, DATE_OCCURRED) AS date FROM [MICT_ELCM].[dbo].[FMDS_ERRORS] where DATE_OCCURRED between '2017-01-01 15:13:51.0870000' AND '2018-01-01 17:05:29.8470000' ORDER BY DATE_OCCURRED"
            ErrorMessage = "SELECT COUNT(ERROR_MESSAGE) as NumberOfError, ERROR_MESSAGE FROM dbo.FMDS_ERRORS WHERE DATE_OCCURRED BETWEEN '2017-01-01 15:13:51.0870000' AND '2018-01-01 17:05:29.8470000' GROUP BY ERROR_MESSAGE ORDER BY NumberOfError DESC"
            cursor.execute(q1)
            Errors = cursor.fetchall()
            cursor.execute(ErrorMessage)
            ErrorMessage = cursor.fetchall()
        GroupByErrorType = {}
        GroupByErrorMessage = {}
        # Start grouping by ErrorType
        print("Start grouping by ErrorType")
        for row in Errors:
            if row[2] not in GroupByErrorType:
                GroupByErrorType[row[2]] = {'M': 0, 'O': 0, 'R4': 0, 'R5': 0, 'W': 0}
            else:
                GroupByErrorType[row[2]][row[0].strip()] += 1
        print("Writing data to csv...")
        with open("C:\Code\ELCM\Data\GroupByErrorType.csv", 'w') as f:
            f.write('Date,M,O,R4,R5,W\n')
            for date, errors in GroupByErrorType.items():
                f.write(date + ',' + str(errors['M']) + ',' + str(errors['O']) + ',' + str(errors['R4']) + ',' + str(errors['R5']) + ',' + str(errors['W']))
                f.write('\n')
        # Start grouping by ErrorMessage
        print("Start grouping by ErrorType")
        # Construct initial dict
        for row in Errors:
            if row[2] not in GroupByErrorMessage:
                GroupByErrorMessage[row[2]] = TempDict(ErrorMessage).dict
                GroupByErrorMessage[row[2]][row[1].strip()] += 1
            else:
                GroupByErrorMessage[row[2]][row[1].strip()] += 1
        # Write to csv
        cols = [m[1].strip() for m in ErrorMessage]
        print("Writing data to csv...")
        with open("C:\Code\ELCM\Data\GrouprByErrorMessage.csv", 'w') as f:
            f.write('Date,')
            for c in cols[:-1]:
                try:
                    f.write(c)
                    f.write(',')
                except UnicodeError:
                    f.write('Power In-Feed DC/DC Converter G23 Error')
            f.write(cols[-1])
            f.write('\n')
            for date, errors in GroupByErrorMessage.items():
                temp_str = ""
                temp_str += date + ','
                for c in cols:
                        temp_str += str(errors[c]) + ','
                temp_str = temp_str[:-1] + '\n'
                f.write(temp_str)
        # Plot  
        """          
            M = []
            O = []
            R4 = []
            R5 = []
            W = []
            for errors in GroupByErrorType.values():
                M.append(errors['M'])
                O.append(errors['O'])
                R4.append(errors['R4'])
                R5.append(errors['R5'])
                W.append(errors['W'])
            ind = len(list(GroupByErrorType))
            width = 0.35
            p1 = plt.bar(ind, M, width)
            p2 = plt.bar(ind, O, width)
            p3 = plt.bar(ind, R4, width)
            p4 = plt.bar(ind, R5, width)
            p5 = plt.bar(ind, W, width)
            plt.ylabel('Number of Errors')
            plt.title('Number of Errors by Error Type')
            plt.xticks(np.arange(len(GroupByErrorType)) , list(GroupByErrorType))
            plt.legend((p1[0], p2[0], p3[0], p4[0], p5[0]), ('M', 'O', 'R4', 'R5', 'W'))
            plt.show()
        """

    def AllErrorTS(self):
        AllErrorTS = {}
        connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"
        # Get all error messages and errors
        with pypyodbc.connect(connection_string, autocommit = True) as conn:
            print("Retrieving data from database")
            cursor = conn.cursor()
            q1 = "SELECT COUNT(ERROR_MESSAGE) as NumberOfError, ERROR_MESSAGE FROM dbo.FMDS_ERRORS GROUP BY ERROR_MESSAGE ORDER BY NumberOfError DESC"
            q2 = "SELECT ERROR_MESSAGE, CONVERT(DATE, DATE_OCCURRED) AS date FROM dbo.FMDS_ERRORS ORDER BY date"
            print("Retrieving error messages...")
            cursor.execute(q1)
            ErrorMessage = cursor.fetchall()
            print("Retrieving all errors...")
            cursor.execute(q2)
            AllErrors = cursor.fetchall()
        for row in AllErrors:
            e = row[0].strip()
            date = row[1]
            if date not in AllErrorTS:
                AllErrorTS[date] = TempDict(ErrorMessage).dict
            AllErrorTS[date][e] += 1
        # Write to csv
        print("Writing to csv...")
        cols = [m[1].strip() for m in ErrorMessage]
        with open("C:\Code\ELCM\Data\AllErrorsTS.csv", 'w') as f:
            f.write('Date,')
            for c in cols[:-1]:
                try:
                    f.write(c)
                    f.write(',')
                except UnicodeError:
                    f.write('Power In-Feed DC/DC Converter G23 Error')
            f.write(cols[-1])
            f.write('\n')
            for date, errors in AllErrorTS.items():
                temp_str = ""
                temp_str += date + ','
                for c in cols:
                        temp_str += str(errors[c]) + ','
                temp_str = temp_str[:-1] + '\n'
                f.write(temp_str)



def main():
    # Boot
    print("Start analyzing ELCM data")
    S = EAD()
    S.AllErrorTS()
    print("Analyzing finished")


if __name__ == '__main__':
    main()
