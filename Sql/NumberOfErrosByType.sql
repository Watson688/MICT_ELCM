/****** Script for SelectTopNRows command from SSMS  ******/
SELECT ERROR_TYPE, COUNT(*) AS Total from dbo.FMDS_ERRORS group by ERROR_TYPE