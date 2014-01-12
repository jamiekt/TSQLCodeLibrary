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

GO
EXEC sp_addextendedproperty @level0name='jt',@level0type='SCHEMA',@level1name='GetTableRowTallies',@level1type='PROCEDURE',@name='CodeLibraryDescription',@value='Tally of rows per table in a given database.';
