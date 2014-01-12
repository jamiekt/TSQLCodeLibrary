CREATE PROCEDURE jt.GetPartitionRowTallies
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

GO
EXEC sp_addextendedproperty @level0name='jt',@level0type='SCHEMA',@level1name='GetPartitionRowTallies',@level1type='PROCEDURE',@name='CodeLibraryDescription',@value='Tally of rows per partition in a given database.';
