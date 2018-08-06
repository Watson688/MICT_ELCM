SELECT RECORD_ID, DATE_EVENT, DEVICE_ID, ITEMNAME, ITEMVALUE, CONVERT(date, DATE_EVENT) AS DATE
into dbo.EVENTS_POSITION
FROM dbo.FMDS_EVENTS_2018
WHERE ITEMNAME = 'PositionX,PositionY,Velocity,Arc'
ORDER BY DATE_EVENT



SELECT RECORD_ID, DATE_EVENT, DEVICE_ID, ITEMNAME, ITEMVALUE, CONVERT(date, DATE_EVENT) AS DATE
into dbo.EVENTS_DEFECT
FROM dbo.FMDS_EVENTS_2018
WHERE ITEMNAME = 'DefectTPX,DefectTPY,DefectTPDate,DefectTPTime,DefectTPAntennaPos'
ORDER BY DATE_EVENT


SELECT RECORD_ID, DATE_EVENT, DEVICE_ID, ITEMNAME, ITEMVALUE, CONVERT(date, DATE_EVENT) AS DATE
into dbo.EVENTS_PLCBAT
FROM dbo.FMDS_EVENTS_2018
WHERE ITEMNAME = 'PLCBrakeResistorFrontTemperature' or ITEMNAME = 'PLCBrakeResistorRearTemperaturee' or ITEMNAME = 'Batt1StateOfCharge,Batt2StateOfCharge,Batt3StateOfCharge,Batt4StateOfCharge,Batt5StateOfCharge,Batt6StateOfCharge,Batt7StateOfCharge,Batt8StateOfCharge,Batt9StateOfCharge'
ORDER BY DATE_EVENT


SELECT RECORD_ID, DATE_EVENT, DEVICE_ID, ITEMNAME, ITEMVALUE, CONVERT(date, DATE_EVENT) AS DATE
into dbo.EVENTS_REST
FROM dbo.FMDS_EVENTS_2018
WHERE ITEMNAME != 'PLCBrakeResistorFrontTemperature' 
AND 
ITEMNAME != 'PLCBrakeResistorRearTemperaturee' 
AND 
ITEMNAME != 'Batt1StateOfCharge,Batt2StateOfCharge,Batt3StateOfCharge,Batt4StateOfCharge,Batt5StateOfCharge,Batt6StateOfCharge,Batt7StateOfCharge,Batt8StateOfCharge,Batt9StateOfCharge'
AND
ITEMNAME != 'DefectTPX,DefectTPY,DefectTPDate,DefectTPTime,DefectTPAntennaPos'
AND
ITEMNAME != 'PositionX,PositionY,Velocity,Arc'
ORDER BY DATE_EVENT
