CREATE PROCEDURE jt.GetTableRowTallies
	@dbName SYSNAME = 'master'
AS
BEGIN
		DECLARE @sql NVARCHAR(MAX) = '
		SELECT t.name
			,s.partition_number
			,s.row_count
		FROM [@dbname].sys.dm_db_partition_stats AS s
			INNER JOIN [@dbname].sys.tables AS t ON t.[object_id] = s.[object_id]
		GROUP BY t.name
			,s.partition_number
			,s.row_count;
		';
		SET	@sql = REPLACE(@sql,'@dbName',@dbName)
		EXEC (@sql)
END