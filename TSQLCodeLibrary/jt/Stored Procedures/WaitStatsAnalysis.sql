CREATE PROCEDURE [dbo].[WaitStatsAnalysis]
		@ResetWaitStats BIT = 0
AS
BEGIN
		PRINT 'Requires VIEW SERVER STATE';
		PRINT 'From Wait statistics, or please tell me where it hurts by Paul Randal http://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/';
		PRINT '  and';
		PRINT 'when were wait stats last cleared? by Erin Stellato http://www.sqlskills.com/blogs/erin/figuring-out-when-wait-stats-were-last-cleared';
		IF @ResetWaitStats = 1
				DBCC SQLPERF(N'sys.dm_os_wait_stats', CLEAR);
 
		/*Wait statistics, or please tell me where it hurts
		http://www.sqlskills.com/blogs/paul/wait-statistics-or-please-tell-me-where-it-hurts/
		*/
		WITH [Waits] AS
			(SELECT
				[wait_type],
				[wait_time_ms] / 1000.0 AS [WaitS],
				([wait_time_ms] - [signal_wait_time_ms]) / 1000.0 AS [ResourceS],
				[signal_wait_time_ms] / 1000.0 AS [SignalS],
				[waiting_tasks_count] AS [WaitCount],
				100.0 * [wait_time_ms] / SUM (NULLIF([wait_time_ms],0)) OVER() AS [Percentage],
				ROW_NUMBER() OVER(ORDER BY [wait_time_ms] DESC) AS [RowNum]
			FROM sys.dm_os_wait_stats
			WHERE [wait_type] NOT IN (
				N'CLR_SEMAPHORE',    N'LAZYWRITER_SLEEP',
				N'RESOURCE_QUEUE',   N'SQLTRACE_BUFFER_FLUSH',
				N'SLEEP_TASK',       N'SLEEP_SYSTEMTASK',
				N'WAITFOR',          N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
				N'CHECKPOINT_QUEUE', N'REQUEST_FOR_DEADLOCK_SEARCH',
				N'XE_TIMER_EVENT',   N'XE_DISPATCHER_JOIN',
				N'LOGMGR_QUEUE',     N'FT_IFTS_SCHEDULER_IDLE_WAIT',
				N'BROKER_TASK_STOP', N'CLR_MANUAL_EVENT',
				N'CLR_AUTO_EVENT',   N'DISPATCHER_QUEUE_SEMAPHORE',
				N'TRACEWRITE',       N'XE_DISPATCHER_WAIT',
				N'BROKER_TO_FLUSH',  N'BROKER_EVENTHANDLER',
				N'FT_IFTSHC_MUTEX',  N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
				N'DIRTY_PAGE_POLL',  N'SP_SERVER_DIAGNOSTICS_SLEEP')
			)
		SELECT
			[W1].[wait_type] AS [WaitType],
			CAST ([W1].[WaitS] AS DECIMAL(14, 2)) AS [Wait_S],
			CAST ([W1].[ResourceS] AS DECIMAL(14, 2)) AS [Resource_S],
			CAST ([W1].[SignalS] AS DECIMAL(14, 2)) AS [Signal_S],
			[W1].[WaitCount] AS [WaitCount],
			CAST ([W1].[Percentage] AS DECIMAL(4, 2)) AS [Percentage],
			CAST (([W1].[WaitS] / NULLIF([W1].[WaitCount],0)) AS DECIMAL (14, 4)) AS [AvgWait_S],
			CAST (([W1].[ResourceS] / NULLIF([W1].[WaitCount],0)) AS DECIMAL (14, 4)) AS [AvgRes_S],
			CAST (([W1].[SignalS] / NULLIF([W1].[WaitCount],0)) AS DECIMAL (14, 4)) AS [AvgSig_S]
		FROM [Waits] AS [W1]
		INNER JOIN [Waits] AS [W2]
			ON [W2].[RowNum] <= [W1].[RowNum]
		GROUP BY [W1].[RowNum], [W1].[wait_type], [W1].[WaitS],
			[W1].[ResourceS], [W1].[SignalS], [W1].[WaitCount], [W1].[Percentage]
		HAVING SUM ([W2].[Percentage]) - [W1].[Percentage] < 95; -- percentage threshold

 
		/* when were wait stats last cleared? from Erin Stellato http://www.sqlskills.com/blogs/erin/figuring-out-when-wait-stats-were-last-cleared/ */
		SELECT	[wait_type],
				[wait_time_ms],
				DATEADD(ms,-[wait_time_ms],getdate()) AS [Date/TimeCleared],
				CASE
				WHEN [wait_time_ms] < 1000 THEN CAST([wait_time_ms] AS VARCHAR(15)) + ' ms'
				WHEN [wait_time_ms] between 1000 and 60000 THEN CAST(([wait_time_ms]/1000) AS VARCHAR(15)) + ' seconds'
				WHEN [wait_time_ms] between 60001 and 3600000 THEN CAST(([wait_time_ms]/60000) AS VARCHAR(15)) + ' minutes'
				WHEN [wait_time_ms] between 3600001 and 86400000 THEN CAST(([wait_time_ms]/3600000) AS VARCHAR(15)) + ' hours'
				WHEN [wait_time_ms] > 86400000 THEN CAST(([wait_time_ms]/86400000) AS VARCHAR(15)) + ' days'
		END [TimeSinceCleared]
		FROM [sys].[dm_os_wait_stats]
		WHERE [wait_type] = 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP';

		/* check SQL Server start time - 2008 and higher */
		SELECT	[sqlserver_start_time]
		FROM	[sys].[dm_os_sys_info];
END