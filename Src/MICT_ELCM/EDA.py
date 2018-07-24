import pypyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class EAD(object):
    def __init__(self):
        pass

    def DailyError(self):
        connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"
        with pypyodbc.connect(connection_string, autocommit = True) as conn:
            # Retrieve data from database
            print("Retrieving data from database")
            cursor = conn.cursor()
            q1 = "SELECT ERROR_TYPE, ERROR_MESSAGE, CONVERT(DATE, DATE_OCCURRED) AS date FROM [MICT_ELCM].[dbo].[FMDS_ERRORS]  where DATE_OCCURRED between '2017-01-01 15:13:51.0870000' AND '2018-01-01 17:05:29.8470000' ORDER BY DATE_OCCURRED"
            AllErrorMessage = "SELECT Distinct ERROR_MESSAGE FROM dbo.FMDS_ERRORS"
            cursor.execute(q1)
            Errors = cursor.fetchall()
            cursor.execute(AllErrorMessage)
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
        with open("C:\Code\MICT_ELCM\Data\GroupByErrorType.csv", 'w') as f:
            f.write('Date,M,O,R4,R5,W\n')
            for date, errors in GroupByErrorType.items():
                f.write(date + ',' + str(errors['M']) + ',' + str(errors['O']) + ',' + str(errors['R4']) + ',' + str(errors['R5']) + ',' + str(errors['W']))
                f.write('\n')
        # Start grouping by ErrorMessage
        print("Start grouping by ErrorType")
        # Construct initial dict
        temp_dict = {}
        for m in ErrorMessage:
            temp_dict[m[0].strip()] = 0
        for row in Errors:
            if row[2] not in GroupByErrorMessage:
                GroupByErrorMessage[row[2]] = temp_dict
            else:
                GroupByErrorMessage[row[2]][row[1].strip()] += 1
        # Write to csv
        cols = list(temp_dict)
        print("Writing data to csv...")
        with open("C:\Code\MICT_ELCM\Data\GrouprByErrorMessage.csv", 'w') as f:
            for c in cols:
                f.write(c)
                f.write(',')
            for date, errors in GroupByErrorMessage.items():
                f.write(date + ',')
                for c in cols[:-1]:
                    f.write(errors[c] + ',')
                f.write(errors[-1] + '\n')
                    

        # Plot            
        print("Start grouping by ErrorMessage")
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

                    
                    



def main():
    # Boot
    print("Start analyzing MICT_ELCM data")
    S = EAD()
    S.DailyError()

if __name__ == '__main__':
    main()
