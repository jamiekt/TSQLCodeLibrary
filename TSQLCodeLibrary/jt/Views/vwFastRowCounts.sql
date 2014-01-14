CREATE VIEW dbo.vwFastRowCounts
AS 
SELECT  schm.name+'.'+objt.name Reference
       ,SUM(row_count) [RowCount]
  FROM sys.dm_db_partition_stats stat
  JOIN sys.objects objt ON stat.object_id = objt.object_id
  JOIN sys.schemas schm ON objt.schema_id = schm.schema_id
 WHERE stat.index_id < 2 AND schm.name <> 'sys'
 GROUP BY schm.name+'.'+objt.name
;
