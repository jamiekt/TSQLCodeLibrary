CREATE PROC jt.TableStats 
	@db_name sysname 
AS
/*
I wanted a way to return the number of rows for all tables in a database. The following did the job:
	exec sp_MSforeachtable 'select COUNT(*) from ?'
but then I figured why not get lots more info while I'm at it. Hence, sp_tablestats.
For every table in the database it returns:
	Table Name
	Rowcount
	Does it have a clustered index?
	Is the primary key clustered?
	Number of columns
	Number of unique, none-primary keys
	Number of none unique columns
	
Use like so:
exec sp_tablestats 'AdventureWorks'

-Jamie Thomson
jamie@jamie-thomson.net
2008-08-18
*/

--DECLARE	@db_name	sysname;
--SET	@db_name = 'DeadlockDemo';

	
	SET NOCOUNT ON
	DECLARE	@tables TABLE
	(
		tablename	sysname
	,	schemaname	sysname
	)
	DECLARE	@tableName					sysname
	,		@schemaname					sysname
	,		@hasCI						BIT
	,		@UK_none_PK					TINYINT
	,		@IsPKClustered				BIT
	,		@NoOfCols					INT
	,		@NoOfCompCols				INT
	,		@NumberOfNoneUniqueIndexes	INT
	,		@NoOfChecks					INT;
	DECLARE @SQLString			nvarchar(500);
	DECLARE	@table_metrics		TABLE
	(
		SchemaName							sysname
	,	TableName							sysname
	,	[RowCount]							int
	,	HasClusteredIndex					bit
	,	IsPKClustered						bit
	,	NumberOfUniqueNonePrimaryIndexes	tinyint
	,	NumberOfNoneUniqueIndexes			tinyint
	,	NumberOfColumns						INT
	,	NumberOfComputedColumns				INT
	,	NumberOfCheckConstraints			INT
	)

	--Check valid DB
	IF NOT EXISTS (SELECT * FROM sys.databases where name = @db_name)
	BEGIN
			PRINT	'Database ' + @db_name + ' does not exist!';
			RETURN;
	END

	--Build list of tables to process
	DECLARE	@tablesSQL	NVARCHAR(500);
	SET		@tablesSQL = '
			SELECT	t.name as tablename, s.name as schemaname
			FROM	' + @db_name + '.sys.tables t
			INNER	JOIN ' + @db_name + '.sys.schemas s
			ON		t.schema_id = s.schema_id
			WHERE	t.type = ''U''
			AND		t.name <> ''sysdiagrams''
			ORDER	BY 2,1';
	INSERT	@tables 
	EXEC	sp_executesql @tablesSQL;
	
	DECLARE tables_curs CURSOR
	FOR		SELECT	tablename,schemaname
			FROM	@tables

	OPEN	tables_curs;

	FETCH	NEXT FROM tables_curs INTO	@tableName, @schemaname

	WHILE @@FETCH_STATUS = 0
	BEGIN
		DECLARE @ParmDefinition nvarchar(500);
		DECLARE @rowcount varchar(30);
		SET		@rowcount = 0

		--How many rows?
		SET @SQLString = N'select @rowcountOUT = COUNT(*) from ' + @db_name + '.' + @schemaname + '.' + @tableName;
		SET @ParmDefinition = N'@rowcountOUT int OUTPUT';
		EXECUTE sp_executesql @SQLString, @ParmDefinition, @rowcountOUT = @rowcount OUTPUT;

		--Has Clustered index?
		SET		@SQLString = N'
				select	@hasCI_OUT = CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END
				from	' + @db_name + '.sys.indexes i
				inner	join ' + @db_name + '.sys.objects o
				on		i.object_id = o.object_id
				inner	join ' + @db_name + '.sys.schemas s
				on		o.schema_id = s.schema_id
				where	i.type = 0
				and		o.Name = ''' + @tableName + '''
				and		s.Name = ''' + @schemaname + '''';
		SET		@ParmDefinition = N'@hasCI_OUT BIT OUTPUT';
		EXEC	sp_executesql @SQLString, @ParmDefinition, @hasCI_OUT = @hasCI OUTPUT;

		--How many unique, none-primary indexes?
		SET		@SQLString = N'
				select	@UK_none_PK_OUT = COUNT(*)
				from	' + @db_name + '.sys.indexes i
				inner	join ' + @db_name + '.sys.objects o
				on		i.object_id = o.object_id
				inner	join ' + @db_name + '.sys.schemas s
				on		o.schema_id = s.schema_id
				where	i.is_unique_constraint = 1
				and		o.Name = ''' + @tableName + '''
				and		s.Name = ''' + @schemaname + '''';
		SET		@ParmDefinition = N'@UK_none_PK_OUT TINYINT OUTPUT';
		EXEC	sp_executesql @SQLString, @ParmDefinition, @UK_none_PK_OUT = @UK_none_PK OUTPUT;

		--How many none unique indexes?
		SET		@SQLString = N'
				select	@none_UK_OUT = COUNT(*)
				from	' + @db_name + '.sys.indexes i
				inner	join ' + @db_name + '.sys.objects o
				on		i.object_id = o.object_id
				inner	join ' + @db_name + '.sys.schemas s
				on		o.schema_id = s.schema_id
				where	i.is_unique_constraint = 0
				and		i.is_primary_key = 0
				and		o.Name = ''' + @tableName + '''
				and		s.Name = ''' + @schemaname + '''';
		SET		@ParmDefinition = N'@none_UK_OUT TINYINT OUTPUT';
		EXEC	sp_executesql @SQLString, @ParmDefinition, @none_UK_OUT = @NumberOfNoneUniqueIndexes OUTPUT;
		
		--Is PK clustered?
		SET		@SQLString = N'
				SELECT	@IsPKClustered_OUT = CASE WHEN COUNT(*) = 1 THEN 1 ELSE 0 END
				FROM	' + @db_name + '.sys.indexes i
				inner	join ' + @db_name + '.sys.objects o
				on		i.object_id = o.object_id
				inner	join ' + @db_name + '.sys.schemas s
				on		o.schema_id = s.schema_id
				WHERE	i.is_primary_key = 1
				AND		i.type = 1
				AND		o.Name = ''' + @tableName + '''
				and		s.Name = ''' + @schemaname + '''';
		SET		@ParmDefinition = N'@IsPKClustered_OUT BIT OUTPUT';
		EXEC	sp_executesql @SQLString, @ParmDefinition, @IsPKClustered_OUT = @IsPKClustered OUTPUT;


		--Number of columns
		SET		@SQLString = N'
				SELECT	@NoOfColsOUT = COUNT(*)
				FROM	' + @db_name + '.sys.columns c
				inner	join ' + @db_name + '.sys.objects o
				on		c.object_id = o.object_id
				inner	join ' + @db_name + '.sys.schemas s
				on		o.schema_id = s.schema_id
				WHERE	o.Name = ''' + @tableName + '''
				and		s.Name = ''' + @schemaname + '''';
		SET		@ParmDefinition = N'@NoOfColsOUT INT OUTPUT';
		EXEC	sp_executesql @SQLString, @ParmDefinition, @NoOfColsOUT = @NoOfCols OUTPUT;
		
		--Number of computed columns
		SET		@SQLString = N'
				SELECT	@NoOfCompCols_OUT = COUNT(*)
				FROM	' + @db_name + '.sys.computed_columns c
				inner	join ' + @db_name + '.sys.objects o
				on		c.object_id = o.object_id
				inner	join ' + @db_name + '.sys.schemas s
				on		o.schema_id = s.schema_id
				WHERE	o.Name = ''' + @tableName + '''
				and		s.Name = ''' + @schemaname + '''';
		SET		@ParmDefinition = N'@NoOfCompCols_OUT INT OUTPUT';
		EXEC	sp_executesql @SQLString, @ParmDefinition, @NoOfCompCols_OUT = @NoOfCompCols OUTPUT;

		--Number of check constraints
		SET		@SQLString = N'
				SELECT	@NoOfChecks_OUT = COUNT(*)
				FROM	' + @db_name + '.sys.check_constraints c
				inner	join ' + @db_name + '.sys.objects o
				on		c.parent_object_id = o.object_id
				inner	join ' + @db_name + '.sys.schemas s
				on		o.schema_id = s.schema_id
				WHERE	o.Name = ''' + @tableName + '''
				and		s.Name = ''' + @schemaname + '''';
		SET		@ParmDefinition = N'@NoOfChecks_OUT INT OUTPUT';
		EXEC	sp_executesql @SQLString, @ParmDefinition, @NoOfChecks_OUT = @NoOfChecks OUTPUT;

		INSERT	@table_metrics (SchemaName, TableName,[RowCount],HasClusteredIndex,NumberOfUniqueNonePrimaryIndexes,IsPKClustered, NumberOfColumns, NumberOfComputedColumns, NumberOfNoneUniqueIndexes, NumberOfCheckConstraints) 
		VALUES (@schemaname, @tableName, @rowcount, @hasCI, @UK_none_PK, @IsPKClustered, @NoOfCols, @NoOfCompCols, @NumberOfNoneUniqueIndexes, @NoOfChecks);
		FETCH	NEXT FROM tables_curs INTO @tableName, @schemaname;
	END

	CLOSE	tables_curs;
	DEALLOCATE tables_curs;

	SELECT	*
	FROM	@table_metrics;

;