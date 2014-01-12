create view jt.vwBufferUsagePerDB
AS
SELECT top 100 percent
[DatabaseName],
      ISNULL([Dirty],'0') AS [Dirty],
      ISNULL([Clean],'0') AS [Clean],
      ISNULL([Total],'0') AS [Total]
FROM(
SELECT
  (CASE WHEN ([database_id] = 32767) THEN 'Resource Database'
            ELSE ISNULL(DB_NAME (database_id),'Total')
      END) AS 'DatabaseName',
  (CASE WHEN ([is_modified] = 1) THEN 'Dirty'
            WHEN ([is_modified] = 0) THEN 'Clean'
            ELSE 'Total'
      END) AS 'State',
  COUNT (*)/128 AS 'SizeInMB'
FROM sys.dm_os_buffer_descriptors
GROUP BY [database_id], [is_modified] WITH CUBE
) AS SourceTable
PIVOT(SUM([SizeInMB]) FOR [State] IN (Clean, Dirty, Total)) AS PivotTable
ORDER BY [DatabaseName]
GO
EXEC sp_addextendedproperty @level0name='jt',@level0type='SCHEMA',@level1name='vwBufferUsagePerDB',@level1type='VIEW',@name='CodeLibraryDescription',@value='Show buffer usage per database.';
