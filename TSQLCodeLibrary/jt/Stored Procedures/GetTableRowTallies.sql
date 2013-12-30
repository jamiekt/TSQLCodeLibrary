CREATE PROCEDURE jt.GetTableRowTallies
	@dbName SYSNAME = 'master'
AS
BEGIN
		CREATE TABLE #t (
				name sysname
			,	partition_number int
			,	row_count int
		);
		INSERT #t
		EXEC jt.GetPartitionRowTallies @dbName = @dbName;
		SELECT	name,row_count = SUM(row_count)
		FROM	#t
		GROUP	BY name;
END