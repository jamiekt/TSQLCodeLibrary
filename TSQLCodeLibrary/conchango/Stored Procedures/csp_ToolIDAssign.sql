/*
<objectname>csp_ToolIDAssign</objectname> 
<summary> 
	Assigns unqiue surrogate key
</summary> 
<parameters>
	<param name="@pTableName">Table Name</param> 
	<param name="@pColumnName">Surrogate Column Name</param> 
	<param name="@pSourceTableName">Source Table of last value</param> 
	<param name="@pConstraint">Additional Constraint</param> 
	<param name="@pPrintSQL">Whether or not to print the SQL</param> 
	<param name="@pExecuteSQL">Whether or not to execute the SQL</param> 
	<param name="@pSQL">The SQL used to generate the surrogate keys</param> 
</parameters>
<history>  
	<entry version="1.0.0.0" date="2004-01-01" name="Conchango" action="Created"/> 
</history> 
*/
CREATE    PROC [conchango].[csp_ToolIDAssign]
(	
	@pTableName			SYSNAME,
	@pColumnName		SYSNAME,
	@pSourceTableName	SYSNAME = NULL, --Optional Source Table Name (ie Presentation table)
	@pConstraint		VARCHAR(1000) = NULL, --Optional Constraint
	@pPrintSQL			TINYINT = 0,
	@pExecuteSQL		TINYINT = 1,
	@pSQL				VARCHAR(1000) = '' OUTPUT
) AS

SET NOCOUNT ON
SET ANSI_WARNINGS ON
SET DATEFORMAT DMY

--Declare Variables
DECLARE @vError			      INT,
		@vSQL			            VARCHAR(1000),
    @vTableSchema         SYSNAME,
    @vTableNameWithoutSchema SYSNAME,
    @vTargetColumnDataType SYSNAME

BEGIN TRY

	--Source Table = Table if not supplied
	SET @pSourceTableName = ISNULL(@pSourceTableName, @pTableName)
	SET @pConstraint = ISNULL(@pConstraint, '')

  IF CHARINDEX('.', @pTableName) <> 0   --i.e. schema name is included in table name
  BEGIN
      SET @vTableSchema = LEFT(@pTableName, CHARINDEX('.', @pTableName) - 1)
      SET @vTableNameWithoutSchema = SUBSTRING(@pTableName, CHARINDEX('.', @pTableName) + 1, 255)
  END
  ELSE
  BEGIN
      SET @vTableSchema = 'dbo'
      SET @vTableNameWithoutSchema = @pTableName
  END
      
  --establish if target column is bigint or not (bigint columns must be handled separately)
  SELECT @vTargetColumnDataType = DATA_TYPE
  FROM  INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_NAME = @vTableNameWithoutSchema
  AND   TABLE_SCHEMA = @vTableSchema
  AND   COLUMN_NAME = @pColumnName

  SET @vSQL = REPLACE('  DECLARE @vKeyCounter	@pDataType', '@pDataType', @vTargetColumnDataType)

  SET @vSQL = @vSQL + '

	SET 	@vKeyCounter = (SELECT ISNULL(MIN(@pColumnName), -1) FROM @pSourceTableName)

	UPDATE 	@pTableName
	SET		@vKeyCounter = @pColumnName = @vKeyCounter - 1
	WHERE	@pColumnName IS NULL
	@pConstraint
	OPTION	(MAXDOP 1)

	'
	SET	@vSQL = REPLACE(@vSQL, '@pColumnName', @pColumnName)
	SET	@vSQL = REPLACE(@vSQL, '@pTableName', @pTableName)
	SET	@vSQL = REPLACE(@vSQL, '@pSourceTableName', @pSourceTableName)
	SET	@vSQL = REPLACE(@vSQL, '@pConstraint', @pConstraint)

	--Return SQL
	SET	@pSQL = @vSQL
	EXEC	dbo.csp_ToolSQLExecute @vSQL, @pPrintSQL, @pExecuteSQL


END TRY
BEGIN CATCH

	EXEC	@vError = csp_LogAndRethrowError
	RETURN	@vError

END CATCH