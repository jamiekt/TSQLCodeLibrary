CREATE PROCEDURE [jt].[AutoGrowAutoShrinkEvents]
@dbname SYSNAME = NULL
AS 
    BEGIN
        IF (@dbname IS NULL) SET @dbname = DB_NAME()  
        BEGIN TRY  
            IF (
                 SELECT CONVERT(INT, value_in_use)
                 FROM   sys.configurations
                 WHERE  name = 'default trace enabled'
               ) = 1 
                BEGIN 
                    DECLARE @curr_tracefilename VARCHAR(500); 
                    DECLARE @base_tracefilename VARCHAR(500); 
                    DECLARE @indx INT;
 
                    SELECT  @curr_tracefilename = path
                    FROM    sys.traces
                    WHERE   is_default = 1; 
                    SET @curr_tracefilename = REVERSE(@curr_tracefilename);
                    SELECT  @indx = PATINDEX('%\%', @curr_tracefilename);
                    SET @curr_tracefilename = REVERSE(@curr_tracefilename);
                    SET @base_tracefilename = LEFT(@curr_tracefilename, LEN(@curr_tracefilename) - @indx) + '\log.trc';  
 
                    SELECT  ( DENSE_RANK() OVER ( ORDER BY StartTime DESC ) ) % 2 AS l1
                    ,       CONVERT(INT, EventClass) AS EventClass
                    ,       DatabaseName
                    ,       FileName
                    ,       ( Duration / 1000 ) AS DurationMS
                    ,       StartTime
                    ,       EndTime
                    ,       ( IntegerData * 8.0 / 1024 ) AS ChangeInSize
                    INTO    #events
                    FROM    ::
                            fn_trace_gettable(@base_tracefilename, DEFAULT)
                    WHERE   EventClass >= 92
                            AND EventClass <= 95
                            AND ServerName = @@servername
                            AND DatabaseName = @dbname;   
                    SELECT  *
                    FROM    #events
                    ORDER BY StartTime DESC;
                    SELECT  TotalDuration = 'Total Autogrow/AutoShrink duration = ' + CONVERT(NVARCHAR(20), SUM(DurationMS)) + 'milliseconds'
                    FROM    #events
                END
            ELSE
				PRINT 'default trace not enabled'; 
                --SELECT  -1 AS l1
                --,       0 AS EventClass
                --,       0 DatabaseName
                --,       0 AS Filename
                --,       0 AS Duration
                --,       0 AS StartTime
                --,       0 AS EndTime
                --,       0 AS ChangeInSize 
        END TRY 
        BEGIN CATCH 
            SELECT  -100 AS l1
            ,       ERROR_NUMBER() AS EventClass
            ,       ERROR_SEVERITY() DatabaseName
            ,       ERROR_STATE() AS Filename
            ,       ERROR_MESSAGE() AS Duration
            ,       1 AS StartTime
            ,       1 AS EndTime
            ,       1 AS ChangeInSize 
        END CATCH
    END
 