import pypyodbc


connection_string = "Driver={SQL Server};Server=princeton;Database=MICT_ELCM;UID=sa;PWD=%5qlish!;"
with pypyodbc.connect(connection_string) as conn:
    columns = "RECORD_ID,DATE_EVENT,DEVICE_ID,ITEMNAME,DEFECTX,DEFECTY,DEFECTTIME,TYPE,X,Y,VOLOCITY,ARC"
    cursor = conn.cursor()
    query1 = "SELECT TOP (1000000) * FROM [MICT_ELCM].[dbo].[FMDS_EVENTS_2018] WHERE ITEMNAME = 'DefectTPX,DefectTPY,DefectTPDate,DefectTPTime,DefectTPAntennaPos' ORDER BY DATE_EVENT DESC"
    print("selecting")
    cursor.execute(query1)
    all_events = cursor.fetchall()
    print("selected {} events".format(str(len(all_events))))
    output = []
    item_name = 'PositionX,PositionY,Velocity,Arc'
    for row in all_events:
        temp = [str(r).strip() for r in row]
        date_event = row[1]
        agv = row[2]
        sub_list = temp[-1].split(",")
        temp = temp[:-1] + [sub_list[0]] + [sub_list[1]] + [sub_list[2] + " " + sub_list[3]] + [sub_list[4]]
        temp[3] = temp[3].replace(",", " ")
        query2 = "SELECT TOP(1) ITEMVALUE FROM [MICT_ELCM].[dbo].[FMDS_EVENTS_2018] WHERE DEVICE_ID = '{0}' AND ITEMNAME = '{1}' AND DATE_EVENT < '{2}' ORDER BY DATE_EVENT DESC".format(agv, item_name, date_event)
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

    
