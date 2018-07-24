import pypyodbc
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class EAD(object):
    def __init__(self):
        pass

    def DailyError(self):
        connection_string = "Driver={SQL Server};Server=DESKTOP-G79E7GF;Database=MICT_ELCM;UID=sa;PWD=password;"
        with pypyodbc.connect(connection_string, autocommit = True) as conn:
            cursor = conn.cursor()
            q1 = "SELECT ERROR_TYPE, ERROR_MESSAGE, CONVERT(DATE, DATE_OCCURRED) AS date FROM [MICT_ELCM].[dbo].[FMDS_ERRORS]  where DATE_OCCURRED between '2017-01-01 15:13:51.0870000' AND '2018-01-01 17:05:29.8470000' ORDER BY DATE_OCCURRED"
            cursor.execute(q1)
            NumberOfErrors = cursor.fetchall()
            ErrorByDate = {}
            for row in NumberOfErrors:
                if row[2] not in ErrorByDate:
                    ErrorByDate[row[2]] = {'M': 0, 'O': 0, 'R4': 0, 'R5': 0, 'W': 0}
                else:
                    ErrorByDate[row[2]][row[0].strip()] += 1
            print("Writing to csv...")
            with open("C:\Code\MICT_ELCM\Data\ErrorByDate.csv", 'w') as f:
                f.write('Date,M,O,R4,R5,W\n')
                for date, errors in ErrorByDate.items():
                    f.write(date + ',' + str(errors['M']) + ',' + str(errors['O']) + ',' + str(errors['R4']) + ',' + str(errors['R5']) + ',' + str(errors['W']))
                    f.write('\n')
            print("plotting")
            M = []
            O = []
            R4 = []
            R5 = []
            W = []
            for errors in ErrorByDate.values():
                M.append(errors['M'])
                O.append(errors['O'])
                R4.append(errors['R4'])
                R5.append(errors['R5'])
                W.append(errors['W'])
            ind = len(list(ErrorByDate))
            width = 0.35
            p1 = plt.bar(ind, M, width)
            p2 = plt.bar(ind, O, width)
            p3 = plt.bar(ind, R4, width)
            p4 = plt.bar(ind, R5, width)
            p5 = plt.bar(ind, W, width)
            plt.ylabel('Number of Errors')
            plt.title('Number of Errors by Error Type')
            plt.xticks(np.arange(len(ErrorByDate)) , list(ErrorByDate))
            plt.legend((p1[0], p2[0], p3[0], p4[0], p5[0]), ('M', 'O', 'R4', 'R5', 'W'))

            plt.show()

                    
                    



def main():
    # Boot
    print("Start analysis MICT_ELCM data")
    S = EAD()
    S.DailyError()

if __name__ == '__main__':
    main()
