CREATE PROCEDURE jt.GetIoVirtualFileStatsDeltas
    @dbname SYSNAME = NULL
,   @filename SYSNAME = NULL
,   @inMostRecentXms INT = NULL
AS 
    BEGIN
        DECLARE @MaxSamplems INT;
        IF NOT EXISTS ( SELECT  *
                        FROM    tempdb.sys.tables
                        WHERE   name = 'VirtualFileStats' ) 
            CREATE TABLE [dbo].[VirtualFileStats]
                (
                 [database_id] [smallint] NOT NULL
                ,[file_id] [smallint] NOT NULL
                ,[sample_ms] [int] NOT NULL
                ,[num_of_reads] [bigint] NOT NULL
                ,[num_of_bytes_read] [bigint] NOT NULL
                ,[io_stall_read_ms] [bigint] NOT NULL
                ,[num_of_writes] [bigint] NOT NULL
                ,[num_of_bytes_written] [bigint] NOT NULL
                ,[io_stall_write_ms] [bigint] NOT NULL
                ,[io_stall] [bigint] NOT NULL
                ,[size_on_disk_bytes] [bigint] NOT NULL
                ,[file_handle] [varbinary](8) NOT NULL
                ,CaptureTimestamp DATETIME
                ) 
        INSERT  tempdb.dbo.VirtualFileStats
                (
                 database_id
                ,file_id
                ,sample_ms
                ,num_of_reads
                ,num_of_bytes_read
                ,io_stall_read_ms
                ,num_of_writes
                ,num_of_bytes_written
                ,io_stall_write_ms
                ,io_stall
                ,size_on_disk_bytes
                ,file_handle
                ,CaptureTimestamp
                )
                SELECT  database_id
                ,       file_id
                ,       sample_ms
                ,       num_of_reads
                ,       num_of_bytes_read
                ,       io_stall_read_ms
                ,       num_of_writes
                ,       num_of_bytes_written
                ,       io_stall_write_ms
                ,       io_stall
                ,       size_on_disk_bytes
                ,       file_handle
                ,       SYSDATETIME()
                FROM    sys.dm_io_virtual_file_stats(DEFAULT, DEFAULT)
        SET @MaxSamplems = (
                             SELECT MAX(sample_ms)
                             FROM   tempdb.dbo.VirtualFileStats
                           );
        SELECT  vfs1.database_id
        ,       vfs1.file_id
        ,       vfs1.sample_ms
        ,       sample_ms_delta = vfs1.[sample_ms] - vfs2.[sample_ms]
        ,       [num_of_reads] = vfs1.[num_of_reads] - vfs2.[num_of_reads]
        ,       [num_of_bytes_read] = vfs1.[num_of_bytes_read] - vfs2.[num_of_bytes_read]
        ,       [io_stall_read_ms] = vfs1.[io_stall_read_ms] - vfs2.[io_stall_read_ms]
        ,       [num_of_writes] = vfs1.[num_of_writes] - vfs2.[num_of_writes]
        ,       [num_of_bytes_written] = vfs1.[num_of_bytes_written] - vfs2.[num_of_bytes_written]
        ,       [io_stall_write_ms] = vfs1.[io_stall_write_ms] - vfs2.[io_stall_write_ms]
        ,       [size_on_disk_bytes] = vfs1.[size_on_disk_bytes] - vfs2.[size_on_disk_bytes]
        INTO    #deltas
        FROM    (
                  SELECT    vfs1.database_id
                  ,         vfs1.file_id
                  ,         vfs1.sample_ms
                  ,         vfs1.num_of_reads
                  ,         vfs1.num_of_bytes_read
                  ,         vfs1.io_stall_read_ms
                  ,         vfs1.num_of_writes
                  ,         vfs1.num_of_bytes_written
                  ,         vfs1.io_stall_write_ms
                  ,         vfs1.io_stall
                  ,         vfs1.size_on_disk_bytes
                  ,         vfs1.file_handle
                  ,         vfs1.CaptureTimestamp
                  ,         MAXsample_ms = MAX(vfs2.[sample_ms])
                  FROM      tempdb.dbo.VirtualFileStats vfs1
                  INNER JOIN sys.master_files mf
                            ON vfs1.database_id = mf.[database_id]
                               AND vfs1.[file_id] = mf.[file_id]
                  INNER JOIN tempdb.dbo.VirtualFileStats vfs2
                            ON vfs1.database_id = vfs2.database_id
                               AND vfs1.file_id = vfs2.file_id
                               AND vfs1.[sample_ms] > vfs2.[sample_ms]
                  WHERE     (
                              @dbname IS NULL
                              OR CHARINDEX(DB_NAME(vfs1.[database_id]), @dbname) > 0
                            )
                            AND (
                                  @filename IS NULL
                                  OR CHARINDEX(mf.name, @filename) > 0
                                )
                            AND (
                                  @inMostRecentXms IS NULL
                                  OR @MaxSamplems - vfs1.[sample_ms] <= @inMostRecentXms
                                )
                  GROUP BY  vfs1.database_id
                  ,         vfs1.file_id
                  ,         vfs1.sample_ms
                  ,         vfs1.num_of_reads
                  ,         vfs1.num_of_bytes_read
                  ,         vfs1.io_stall_read_ms
                  ,         vfs1.num_of_writes
                  ,         vfs1.num_of_bytes_written
                  ,         vfs1.io_stall_write_ms
                  ,         vfs1.io_stall
                  ,         vfs1.size_on_disk_bytes
                  ,         vfs1.file_handle
                  ,         vfs1.CaptureTimestamp
                ) vfs1
        INNER JOIN tempdb.dbo.VirtualFileStats vfs2
                ON vfs1.[database_id] = vfs2.[database_id]
                   AND vfs1.[file_id] = vfs2.[file_id]
                   AND vfs1.[MAXsample_ms] = vfs2.[sample_ms];
 
 
 
 
--SELECT DatabaseName=DB_NAME(d.database_id),filename=mf.name,*
--FROM #deltas d
--INNER JOIN sys.master_files mf
--    ON d.[database_id] = mf.[database_id]
--    AND d.[file_id] = mf.[file_id]
--ORDER   BY d.database_id,d.file_id,sample_ms ASC;
 
        SELECT  DatabaseName = DB_NAME(d.database_id)
        ,       filename = mf.name
        ,       sample_ms_delta
        ,       AverageWaitTimePerRead_ms = io_stall_read_ms / NULLIF(num_of_reads, 0)
        ,       AverageWaitTimePerWrite_ms = io_stall_write_ms / NULLIF(num_of_writes, 0)
        FROM    #deltas d
        INNER JOIN sys.master_files mf
                ON d.[database_id] = mf.[database_id]
                   AND d.[file_id] = mf.[file_id]
        ORDER BY d.database_id
        ,       d.file_id
        ,       sample_ms ASC;
 
    END