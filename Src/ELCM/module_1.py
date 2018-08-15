import pypyodbc


connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"
with pypyodbc.connect(connection_string) as conn:
    columns = "RECORD_ID,DATE_EVENT,DEVICE_ID,ITEMNAME,ITEMVALUE,TYPE,X,Y,VOLOCITY,ARC"
    cursor = conn.cursor()
    query1 = "SELECT TOP (100) * FROM [MICT_ELCM].[dbo].[FMDS_EVENTS_2018] WHERE ITEMNAME = 'DefectTPX,DefectTPY,DefectTPDate,DefectTPTime,DefectTPAntennaPos' ORDER BY DATE_EVENT DESC"
    print("selecting")
    cursor.execute(query1)
    all_events = cursor.fetchall()
    print("selected {} events".format(str(len(all_events))))
    output = []
    item_name = 'PositionX,PositionY,Velocity,Arc'
    for row in all_events:
        temp = []
        date_event = row[1].strip()
        agv = row[2].strip()
        for i in row:
            temp.append(str(i).strip())
        type = temp[-1].split(',')[-1]
        temp[-1] = " ".join(temp[-1].split(',')[:-1])
        temp[3] = temp[3].replace(","," ")
        temp.append(type)
        query2 = "SELECT TOP(1) ITEMVALUE FROM [MICT_ELCM].[dbo].[FMDS_EVENTS_2018] WHERE DEVICE_ID = '{0}' AND ITEMNAME = '{1}' AND DATE_EVENT < '{2}'".format(agv, item_name, date_event)
        cursor.execute(query2)
        position = cursor.fetchall()
        if position:
            coordinates = position[0][0].strip().split(",")
            for j in coordinates:
                temp.append(j)
        temp.append('\n')
        output.append(",".join(temp))
    print("writing...")
    with open("defect_position.csv", 'w') as f:
        f.write(columns + '\n')
        for r in output:
            f.write(r)
    print("finished")

    
