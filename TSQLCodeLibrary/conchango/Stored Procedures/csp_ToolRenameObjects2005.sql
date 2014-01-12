CREATE   PROC [conchango].[csp_ToolRenameObjects2005]
(
	@pTableName	SYSNAME = '%',
	@pFK		BIT	= 1,
	@pCheck		BIT	= 1,
	@pIndexes	BIT	= 1,
	@pPrintSQL 	BIT	= 1,
	@pExecuteSQL	BIT	= 0
) as

SET NOCOUNT ON

DECLARE @vTableName 		SYSNAME,
	@vObjectCount		TINYINT,
	@vCurObjectCount	CHAR(2),
	@i			VARCHAR(2),
	@vCurObjectName		SYSNAME,
	@vNewObjectName		SYSNAME,
	@vFKTableName		SYSNAME,
	@vPKTableName		SYSNAME,
	@vPrimary		BIT,
	@vClustered		BIT

IF @pTableName IS NULL OR @pTableName = ''
	SET @pTableName = '%'

DECLARE cur_tables cursor LOCAL FAST_FORWARD FOR
	SELECT 	TABLE_NAME 
	FROM 	INFORMATION_SCHEMA.TABLES
	WHERE 	OBJECTPROPERTY(OBJECT_ID(TABLE_NAME), 'ISMSSHIPPED') = 0 AND
		TABLE_NAME LIKE @pTableName
	ORDER BY 1

OPEN cur_tables

FETCH NEXT FROM cur_tables INTO @vTableName

WHILE @@FETCH_STATUS = 0
BEGIN
	/*
	**	 FK Constraints
	*/

	IF @pFK = 1
	BEGIN
		IF OBJECT_ID('TEMPDB..#FKs') IS NOT NULL
			DROP TABLE #FKs
	
		SELECT 	DISTINCT 
			object_name(constid) 	as CurObjectName, 
			object_name(fkeyid) 	as FKTableName, 
			--Following line changed from csp_ToolRenameObjects
			--object_name(rkeyid)	as PKTableName,*
			object_name(rkeyid)	as PKTableName
		INTO 	#FKs
		FROM 	sysforeignkeys sc 
		WHERE 	fkeyid = OBJECT_ID(@vTableName) 
		ORDER BY object_name(fkeyid), object_name(rkeyid)
		
		SELECT  @vObjectCount = @@ROWCOUNT,
			@i = '1'
	
		DECLARE cur_FKs CURSOR LOCAL FAST_FORWARD FOR
			SELECT 	CurObjectName, FKTableName, PKTableName
			FROM #FKs
	
		OPEN cur_FKs
	
		FETCH NEXT FROM cur_FKs INTO @vCurObjectName, @vFKTableName, @vPKTableName
	
		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @vCurObjectCount = REPLICATE('0', 2 - LEN(@i)) + @i
			
			SELECT @vNewObjectName = 'FK_' + @vFKTableName + '_' + @vPKTableName + '_' + @vCurObjectCount
	
			IF @pExecuteSQL = 1
				EXEC sp_rename @vCurObjectName, @vNewObjectName, 'OBJECT'
	
			IF @pPrintSQL = 1
				SELECT @vNewObjectName, @vCurObjectName		
	
			SET @i = @i + 1
			FETCH NEXT FROM cur_FKs INTO @vCurObjectName, @vFKTableName, @vPKTableName
		END
	
		CLOSE cur_FKs
		DEALLOCATE cur_FKs
	END

	/*
	**	Check Constraints
	*/
	
	IF @pCheck = 1
	BEGIN
		IF OBJECT_ID('TEMPDB..#CHKs') IS NOT NULL
			DROP TABLE #CHKs
	
		SELECT 	DISTINCT 
			CONSTRAINT_NAME AS CurObjectName
		INTO 	#CHKs
		FROM 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
		WHERE 	TABLE_NAME = @vTableName AND
			CONSTRAINT_TYPE = 'CHECK'
		ORDER BY CONSTRAINT_NAME
		
		SELECT  @vObjectCount = @@ROWCOUNT,
			@i = '1'
	
		DECLARE cur_CHKs CURSOR LOCAL FAST_FORWARD FOR
			SELECT 	CurObjectName
			FROM #CHKs
	
		OPEN cur_CHKs
	
		FETCH NEXT FROM cur_CHKs INTO @vCurObjectName
	
		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @vCurObjectCount = REPLICATE('0', 2 - LEN(@i)) + @i
			
			SELECT @vNewObjectName = 'CK_' + @vTableName + '_' + @vCurObjectCount
	
			IF @pExecuteSQL = 1
				EXEC sp_rename @vCurObjectName, @vNewObjectName, 'OBJECT'
	
			IF @pPrintSQL = 1
				SELECT @vNewObjectName, 	@vCurObjectName		
	
			SET @i = @i + 1
			FETCH NEXT FROM cur_CHKs INTO @vCurObjectName
		END
	
		CLOSE cur_CHKs
		DEALLOCATE cur_CHKs
	END
	
	/*
	**	Indexes
	*/
	IF @pIndexes = 1
	BEGIN
		
		IF OBJECT_ID('TEMPDB..#Indexes') IS NOT NULL
			DROP TABLE #Indexes
	
		SELECT 	OBJECT_NAME(si.id) as TableName,
			si.name as IndexName, 
			(CASE WHEN OBJECTPROPERTY(scon.constid, 'IsPrimaryKey') = 1 THEN 1 ELSE 0 END) as bPrimary,
			(CASE WHEN INDEXPROPERTY(si.id, si.name, 'IsClustered') = 1 THEN 1 ELSE 0 END) as bClustered,
			max(sk.keyno) as NumCols
		INTO	#Indexes
		FROM 	sysindexes si
			INNER JOIN 
			sysindexkeys sk
			ON
			si.id = sk.id and
			si.indid = sk.indid
			INNER JOIN
			syscolumns sc
			ON
			sk.id = sc.id and
			sk.colid = sc.colid
			LEFT OUTER JOIN sysconstraints scon
			ON
			si.id = scon.id and
			si.name = object_name(scon.constid)
		WHERE 	INDEXPROPERTY(si.id, si.name, 'IsStatistics') = 0  AND
		 	OBJECT_NAME(si.id) = @vTableName
		GROUP BY OBJECT_NAME(si.id),
			si.name, 
			(CASE WHEN OBJECTPROPERTY(scon.constid, 'IsPrimaryKey') = 1 THEN 1 ELSE 0 END),
			(CASE WHEN INDEXPROPERTY(si.id, si.name, 'IsClustered') = 1 THEN 1 ELSE 0 END)
		ORDER 	BY	
			OBJECT_NAME(si.id),
			(CASE WHEN INDEXPROPERTY(si.id, si.name, 'IsClustered') = 1 THEN 1 ELSE 0 END) DESC,
			si.name
	
		SELECT  @vObjectCount = @@ROWCOUNT,
			@i = '1'
	
		DECLARE cur_Indexes CURSOR LOCAL FAST_FORWARD FOR
			SELECT 	IndexName, bPrimary, bClustered
			FROM #Indexes
	
		OPEN cur_Indexes
	
		FETCH NEXT FROM cur_Indexes INTO @vCurObjectName, @vPrimary, @vClustered
	
		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @vCurObjectCount = REPLICATE('0', 2 - LEN(@i)) + @i
			
			SELECT @vNewObjectName = @vTableName + '_' + (CASE WHEN @vPrimary = 1 THEN 'PK' 
									WHEN @vClustered = 1 THEN 'idx'
									ELSE 'ndx' + @vCurObjectCount END),
				@vCurObjectName = @vTableName + '.' + @vCurObjectName
	
			IF @pExecuteSQL = 1
				EXEC sp_rename @vCurObjectName, @vNewObjectName, 'INDEX'
	
			IF @pPrintSQL = 1
				SELECT @vNewObjectName, 	@vCurObjectName		
	
			IF @vClustered <> 1 AND @vPrimary <> 1
				SET @i = @i + 1
	
			FETCH NEXT FROM cur_Indexes INTO @vCurObjectName, @vPrimary, @vClustered
		END
	
		CLOSE cur_Indexes
		DEALLOCATE cur_Indexes
	
		
		IF OBJECT_ID('TEMPDB..#Indexes') IS NOT NULL
			DROP TABLE #Indexes
	END
	
	-- Next Table
	FETCH NEXT FROM cur_tables INTO @vTableName
END

CLOSE cur_tables
DEALLOCATE cur_tables

/*

select 	top 100 * 
from 	sysforeignkeys
from 	information_schema.referential_constraints
where Constraint_type = 'FOREIGN KEY'

*/