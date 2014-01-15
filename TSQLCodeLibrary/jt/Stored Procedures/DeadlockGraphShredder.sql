CREATE PROCEDURE [jt].[DeadlockGraphShredder]
AS
BEGIN
		IF OBJECT_ID('tempdb..#rawDeadlockGraph') IS NOT NULL 
			DROP TABLE #rawDeadlockGraph;
		SELECT  CAST (XEventData.XEvent.value('(data/value)[1]', 'varchar(max)') AS XML) AS DeadlockGraph
		INTO    #rawDeadlockGraph
		FROM    (
				  SELECT    CAST (target_data AS XML) AS TargetData
				  ,         *
				  FROM      sys.dm_xe_session_targets st
				  JOIN      sys.dm_xe_sessions s
							ON s.address = st.event_session_address
				  WHERE     s.[name] = 'system_health'
				) AS Data
		CROSS APPLY TargetData.nodes('//RingBufferTarget/event') AS XEventData ( XEvent )
		WHERE   XEventData.XEvent.value('@name', 'varchar(4000)') = 'xml_deadlock_report';
		IF OBJECT_ID('tempdb..#deadlockgraphCTE') IS NOT NULL 
			DROP TABLE #deadlockgraphCTE;
		SELECT  q.DeadlockGraph
		,       q.[starttime]
		,       [DeadLockID] = ROW_NUMBER() OVER ( ORDER BY q.[starttime] ASC )
		INTO    #deadlockgraphCTE
		FROM    (
				  SELECT    q.DeadlockGraph
				  ,         [starttime] = q.DeadlockGraph.value('/deadlock[1]/process-list[1]/process[1]/@lasttranstarted[1]', 'varchar(max)')
				  FROM      #rawDeadlockGraph q
				) q;
		IF OBJECT_ID('tempdb..#victimListCTE') IS NOT NULL 
			DROP TABLE #victimListCTE;
		SELECT  [victimProcessid] = q.DeadlockGraph.value('/deadlock[1]/victim-list[1]/victimProcess[1]/@id[1]', 'varchar(max)')
		,       [DeadLockID]
		INTO    #victimListCTE
		FROM    #deadlockgraphCTE q;
		IF OBJECT_ID('tempdb..#processesCTE') IS NOT NULL 
			DROP TABLE #processesCTE;
		SELECT  [DeadLockID]
		,       [starttime]
		,       [processA_processid] = XEventData.XEvent.value('process-list[1]/process[1]/@id', 'varchar(max)')
		,       [processB_processid] = XEventData.XEvent.value('process-list[1]/process[2]/@id', 'varchar(max)')
		INTO    #processesCTE
		FROM    #deadlockgraphCTE
		CROSS APPLY DeadLockGraph.nodes('/deadlock') AS XEventData ( XEvent );
		IF OBJECT_ID('tempdb..#resourceList') IS NOT NULL 
			DROP TABLE #resourceList;
		SELECT  [resource-list] = c.Deadlockgraph.query('//resource-list')
		,       DeadLockID
		INTO    #resourceList
		FROM    #deadlockgraphCTE c
		SELECT  q.[DeadLockID]
		,       [IsVictim] = CASE WHEN vlCTE.[victimProcessid] IS NOT NULL THEN '    *'
							 END
		,       [processid]
		,       [inputbuf] = CONVERT(XML, [inputbuf])
		,       [statementtext(all execution stack)] = CONVERT(XML, [statementtext])
			--,       [querytext(if still in cache)] = CONVERT(XML, [querytext])
		,       [querytext(if still in cache)] = [querytext]
		,       [query_plan(if still in cache)] = [query_plan]
		,       logused
		,       [lockmode]
		,       [currentdbname]
		,       [isolationlevel]
		,       [spid]
		,       [lasttranstarted]
		,       [lastbatchstarted]
		,       [lastbatchcompleted]
		,       [waitresource]
		,       [waitobject] = CASE WHEN LEFT([waitresource], 6) = 'OBJECT' THEN ISNULL((
																						  SELECT    OBJECT_NAME(SUBSTRING(DBandOBJ, CHARINDEX(':', DBandOBJ) + 1, LEN(DBandOBJ)), SUBSTRING(DBandOBJ, 1, CHARINDEX(':', DBandOBJ) - 1))
																						  FROM      (
																									  SELECT    DBandOBJ = REVERSE(SUBSTRING(REVERSE(REPLACE(RTRIM(LTRIM(waitresource)), 'OBJECT: ', '')), 3, LEN(RTRIM(LTRIM(waitresource)))))
																									) q
																						), '<don''t think that object exists anymore>')
									WHEN LEFT([waitresource], 3) = 'KEY' THEN (
																				SELECT  ISNULL(OBJECT_NAME([object_id], [currentdb]), '<you need to execute against [' + [currentdbname] + '] in order to see a value here>')
																				FROM    sys.partitions p
																				INNER JOIN (
																							 SELECT hobt_id = SUBSTRING(DBandOBJ, CHARINDEX(':', DBandOBJ) + 1, CHARINDEX(' ', DBandOBJ) - CHARINDEX(':', DBandOBJ))
																							 FROM   (
																									  SELECT    DBandOBJ = REPLACE(RTRIM(LTRIM(waitresource)), 'KEY: ', '')
																									) q
																						   ) hobt
																						ON p.hobt_id = hobt.hobt_id
																			  )
									WHEN LEFT([waitresource], 8) = 'METADATA'
										 AND CHARINDEX('PARTITION_FUNCTION', [waitresource]) > 0 THEN ( ISNULL((
																												 SELECT ISNULL(pf.[name], '<you need to execute against [' + [currentdbname] + '] in order to see a value here>')
																												 FROM   sys.partition_functions pf
																												 INNER JOIN (
																															  SELECT    function_id = SUBSTRING(DBandOBJ, CHARINDEX('=', DBandOBJ, CHARINDEX('=', DBandOBJ) + 1) + 2, 5)
																															  FROM      (
																																		  SELECT    DBandOBJ = REPLACE(RTRIM(LTRIM(waitresource)), 'METADATA: ', '')
																																		) q
																															) q
																														ON pf.[function_id] = q.[function_id]
																											   ), '<either you need to execute against [' + [currentdbname] + '] in order to see a value here or the object no longer exists>') )
									WHEN LEFT([waitresource], 8) = 'METADATA'
										 AND CHARINDEX('DATA_SPACE', [waitresource]) > 0 THEN ( ISNULL((
																										 SELECT ds.[name]
																										 FROM   sys.data_spaces ds
																										 INNER JOIN (
																													  SELECT    [data_space_id] = SUBSTRING(DBandOBJ, CHARINDEX('=', DBandOBJ, CHARINDEX('=', DBandOBJ) + 1) + 2, 5)
																													  FROM      (
																																  SELECT    DBandOBJ = REPLACE(RTRIM(LTRIM(waitresource)), 'METADATA: ', '')
																																) q
																													) q
																												ON ds.[data_space_id] = q.[data_space_id]
																									   ), '<you need to execute against [' + [currentdbname] + '] in order to see a value here>') )
							   END
		FROM    (
				  SELECT    processAexecutionStack.[DeadLockID]
				  ,         [ProcessID] = processAexecutionStack.[processid]
				  ,         [inputbuf] = processAexecutionStack.[inputbuf]
				  ,         [statementtext] = processAexecutionStack.[statementtext]
				  ,         [querytext] = processAexecutionStack.[querytext]
				  ,         [query_plan] = processAexecutionStack.[query_plan]
				  ,         logused
				  ,         [lockmode]
				  ,         [currentdb]
				  ,         [currentdbname]
				  ,         [isolationlevel]
				  ,         [spid]
				  ,         [waitresource]
				  ,         [lasttranstarted]
				  ,         [lastbatchstarted]
				  ,         [lastbatchcompleted]
				  FROM      #processesCTE processes
				  INNER JOIN (
							   SELECT   executionStack.DeadLockID
							   ,        [statementtext] = SUBSTRING(est.text, ( [stmtstart] / 2 ) + 1, ( ( CASE [stmtend]
																											 WHEN -1 THEN DATALENGTH(est.text)
																											 ELSE [stmtend]
																										   END - [stmtstart] ) / 2 ) + 1)
							   ,        [querytext] = est.text
							   ,        ecp.[query_plan]
							   ,        executionStack.[processid]
							   ,        executionStack.[inputbuf]
							   ,        executionStack.[logused]
							   ,        [lockmode]
							   ,        [currentdb]
							   ,        [currentdbname]
							   ,        [isolationlevel]
							   ,        [spid]
							   ,        [waitresource]
							   ,        [lasttranstarted]
							   ,        [lastbatchstarted]
							   ,        [lastbatchcompleted]
							   FROM     (
										  SELECT    deadlockgraphCTE.[DeadLockID]
										  ,         [line] = XEventData.XEvent.value('@line[1]', 'int')
										  ,         [stmtstart] = XEventData.XEvent.value('@stmtstart[1]', 'int')
										  ,         [stmtend] = XEventData.XEvent.value('@stmtend[1]', 'int')
										  ,         [sqlhandle] = CONVERT(VARBINARY(64), XEventData.XEvent.value('@sqlhandle[1]', 'nvarchar(max)'), 1)
										  ,         [processid] = XEventData2.XEvent.value('process-list[1]/process[1]/@id', 'nvarchar(max)')
										  ,         [inputbuf] = XEventData2.XEvent.value('process-list[1]/process[1]/inputbuf[1]', 'nvarchar(max)')
										  ,         [logused] = XEventData2.XEvent.value('process-list[1]/process[1]/@logused[1]', 'int')
										  ,         [lockmode] = XEventData2.XEvent.value('process-list[1]/process[1]/@lockMode[1]', 'nvarchar(max)')
										  ,         [currentdb] = XEventData2.XEvent.value('process-list[1]/process[1]/@currentdb[1]', 'int')
										  ,         [currentdbname] = DB_NAME(XEventData2.XEvent.value('process-list[1]/process[1]/@currentdb[1]', 'int'))
										  ,         [isolationlevel] = XEventData2.XEvent.value('process-list[1]/process[1]/@isolationlevel[1]', 'nvarchar(max)')
										  ,         [spid] = XEventData2.XEvent.value('process-list[1]/process[1]/@spid[1]', 'int')
										  ,         [waitresource] = XEventData2.XEvent.value('process-list[1]/process[1]/@waitresource[1]', 'nvarchar(max)')
										  ,         [lasttranstarted] = XEventData2.XEvent.value('process-list[1]/process[1]/@lasttranstarted[1]', 'nvarchar(max)')
										  ,         [lastbatchstarted] = XEventData2.XEvent.value('process-list[1]/process[1]/@lastbatchstarted[1]', 'nvarchar(max)')
										  ,         [lastbatchcompleted] = XEventData2.XEvent.value('process-list[1]/process[1]/@lastbatchcompleted[1]', 'nvarchar(max)')
										  FROM      #deadlockgraphCTE deadlockgraphCTE
										  CROSS APPLY DeadLockGraph.nodes('/deadlock/process-list[1]/process[1]/executionStack/frame') AS XEventData ( XEvent )
										  CROSS APPLY DeadLockGraph.nodes('/deadlock') AS XEventData2 ( XEvent )
										) executionStack
							   LEFT OUTER JOIN sys.dm_exec_query_stats eqs
										ON executionStack.[sqlhandle] = eqs.[sql_handle]
							   OUTER APPLY sys.dm_exec_sql_text(executionStack.[sqlhandle]) est
							   OUTER APPLY sys.dm_exec_query_plan(eqs.[plan_handle]) ecp
							 ) processAexecutionStack
							ON processes.[DeadLockID] = processAexecutionStack.[DeadLockID]
				  UNION ALL
				  SELECT    processBexecutionStack.[DeadLockID]
				  ,         [ProcessID] = processBexecutionStack.[processid]
				  ,         [inputbuf] = processBexecutionStack.[inputbuf]
				  ,         [statementtext] = processBexecutionStack.[statementtext]
				  ,         [querytext] = processBexecutionStack.[querytext]
				  ,         [query_plan] = processBexecutionStack.[query_plan]
				  ,         logused
				  ,         [lockmode]
				  ,         [currentdb]
				  ,         [currentdbname]
				  ,         [isolationlevel]
				  ,         [spid]
				  ,         [waitresource]
				  ,         [lasttranstarted]
				  ,         [lastbatchstarted]
				  ,         [lastbatchcompleted]
				  FROM      #processesCTE processes
				  INNER JOIN (
							   SELECT   executionStack.DeadLockID
							   ,        [statementtext] = SUBSTRING(est.text, ( [stmtstart] / 2 ) + 1, ( ( CASE [stmtend]
																											 WHEN -1 THEN DATALENGTH(est.text)
																											 ELSE [stmtend]
																										   END - [stmtstart] ) / 2 ) + 1)
							   ,        [querytext] = est.text
							   ,        ecp.[query_plan]
							   ,        executionStack.[processid]
							   ,        executionStack.[inputbuf]
							   ,        executionStack.[logused]
							   ,        [lockmode]
							   ,        [currentdb]
							   ,        [currentdbname]
							   ,        [isolationlevel]
							   ,        [spid]
							   ,        [waitresource]
							   ,        [lasttranstarted]
							   ,        [lastbatchstarted]
							   ,        [lastbatchcompleted]
							   FROM     (
										  SELECT    deadlockgraphCTE.[DeadLockID]
										  ,         [line] = XEventData.XEvent.value('@line[1]', 'int')
										  ,         [stmtstart] = XEventData.XEvent.value('@stmtstart[1]', 'int')
										  ,         [stmtend] = XEventData.XEvent.value('@stmtend[1]', 'int')
										  ,         [sqlhandle] = CONVERT(VARBINARY(64), XEventData.XEvent.value('@sqlhandle[1]', 'nvarchar(max)'), 1)
										  ,         [processid] = XEventData2.XEvent.value('process-list[1]/process[2]/@id', 'nvarchar(max)')
										  ,         [inputbuf] = XEventData2.XEvent.value('process-list[1]/process[2]/inputbuf[1]', 'nvarchar(max)')
										  ,         [logused] = XEventData2.XEvent.value('process-list[1]/process[2]/@logused[1]', 'int')
										  ,         [lockmode] = XEventData2.XEvent.value('process-list[1]/process[2]/@lockMode[1]', 'nvarchar(max)')
										  ,         [currentdb] = XEventData2.XEvent.value('process-list[1]/process[2]/@currentdb[1]', 'int')
										  ,         [currentdbname] = DB_NAME(XEventData2.XEvent.value('process-list[1]/process[2]/@currentdb[1]', 'int'))
										  ,         [isolationlevel] = XEventData2.XEvent.value('process-list[1]/process[2]/@isolationlevel[1]', 'nvarchar(max)')
										  ,         [spid] = XEventData2.XEvent.value('process-list[1]/process[2]/@spid[1]', 'int')
										  ,         [waitresource] = XEventData2.XEvent.value('process-list[1]/process[2]/@waitresource[1]', 'nvarchar(max)')
										  ,         [lasttranstarted] = XEventData2.XEvent.value('process-list[1]/process[2]/@lasttranstarted[1]', 'nvarchar(max)')
										  ,         [lastbatchstarted] = XEventData2.XEvent.value('process-list[1]/process[2]/@lastbatchstarted[1]', 'nvarchar(max)')
										  ,         [lastbatchcompleted] = XEventData2.XEvent.value('process-list[1]/process[2]/@lastbatchcompleted[1]', 'nvarchar(max)')
										  FROM      #deadlockgraphCTE deadlockgraphCTE
										  CROSS APPLY DeadLockGraph.nodes('/deadlock/process-list[1]/process[2]/executionStack/frame') AS XEventData ( XEvent )
										  CROSS APPLY DeadLockGraph.nodes('/deadlock') AS XEventData2 ( XEvent )
										) executionStack
							   LEFT OUTER JOIN sys.dm_exec_query_stats eqs
										ON executionStack.[sqlhandle] = eqs.[sql_handle]
							   OUTER APPLY sys.dm_exec_sql_text(executionStack.[sqlhandle]) est
							   OUTER APPLY sys.dm_exec_query_plan(eqs.[plan_handle]) ecp
							 ) processBexecutionStack
							ON processes.[DeadLockID] = processBexecutionStack.[DeadLockID]
				) q
		LEFT OUTER JOIN #victimListCTE vlCTE
				ON q.[ProcessID] = vlCTE.[victimProcessid]
				   AND q.[DeadLockID] = vlCTE.[DeadLockID]
		ORDER BY q.[DeadLockID] ASC
		,       [IsVictim] DESC
		,       [processid] ASC;

		SELECT  q.[DeadLockID]
		,       [lockobject] = CASE WHEN q.[locksubresource] = 'FULL' THEN ISNULL(OBJECT_NAME(q.[lockassociatedObjectId], q.[lockdbid]), '<don''t think that object exists anymore>')
									WHEN q.[locksubresource] = 'PARTITION_FUNCTION' THEN ISNULL((
																								  SELECT    ISNULL(pf.[name], '<you need to execute against [' + q.[lockdbname] + '] in order to see a value here>')
																								  FROM      sys.partition_functions pf
																								  WHERE     [function_id] = SUBSTRING(q.[lockclassid], CHARINDEX('=', q.[lockclassid]) + 2, LEN(q.[lockclassid]))
																								), '<you need to execute against [' + q.[lockdbname] + '] in order to see a value here or the object no longer exists>')
									WHEN q.[locksubresource] = 'DATA_SPACE' THEN ISNULL((
																						  SELECT    ds.[name]
																						  FROM      sys.data_spaces ds
																						  WHERE     [data_space_id] = SUBSTRING(q.[lockclassid], CHARINDEX('=', q.[lockclassid]) + 2, LEN(q.[lockclassid]))
																						), '<you need to execute against [' + q.[lockdbname] + '] in order to see a value here or the object no longer exists>')
							   END
		,       q.[lockType]
		,       q.[lockmode]
		--,       q.[lockid]
		--,       q.[lockobjectname]
		--,       q.[lockassociatedObjectId]
		--,       q.[lockPartition]
		,       q.[locksubresource]
		--,       q.[lockclassid]
		,       q.[lockdbname]
		,       q.[ownerid]
		,       q.[ownermode]
		,       q.[waiterid]
		,       q.[waitermode]
		,       q.[waiterrequestType]
		FROM    (
				  SELECT    deadlockgraphCTE.[DeadLockID]
				  ,         [data] = Resources._Resource.query('.')
				  ,         [lockType] = Resources._Resource.value('local-name(.)', 'nvarchar(max)')
				  ,         [lockmode] = Resources._Resource.value('@mode', 'nvarchar(max)')
				  ,         [lockid] = Resources._Resource.value('@id', 'nvarchar(max)')
				  ,         [lockobjectname] = Resources._Resource.value('@objectname', 'nvarchar(max)')
				  ,         [lockassociatedObjectId] = Resources._Resource.value('@associatedObjectId', 'nvarchar(max)')
				  ,         [lockPartition] = Resources._Resource.value('@lockPartition', 'nvarchar(max)')
				  ,         [locksubresource] = Resources._Resource.value('@subresource', 'nvarchar(max)')
				  ,         [lockclassid] = Resources._Resource.value('@classid', 'nvarchar(max)')
				  ,         [lockdbid] = Resources._Resource.value('@dbid', 'int')
				  ,         [lockdbname] = DB_NAME(Resources._Resource.value('@dbid', 'int'))
				  ,         [ownerid] = OwnerList._Owner.value('@id', 'nvarchar(max)')
				  ,         [ownermode] = OwnerList._Owner.value('@mode', 'nvarchar(max)')
				  ,         [waiterid] = WaiterList._Waiter.value('@id', 'nvarchar(max)')
				  ,         [waitermode] = WaiterList._Waiter.value('@mode', 'nvarchar(max)')
				  ,         [waiterrequestType] = WaiterList._Waiter.value('@requestType', 'nvarchar(max)')
				  FROM      #deadlockgraphCTE deadlockgraphCTE
				  CROSS APPLY DeadLockGraph.nodes('//resource-list/*') AS Resources ( _Resource )
				  CROSS APPLY Resources._Resource.nodes('owner-list/owner') AS OwnerList ( _Owner )
				  CROSS APPLY Resources._Resource.nodes('waiter-list/waiter') AS WaiterList ( _Waiter )
				) q

		/*
		understanding waitresource
		http://support.microsoft.com/kb/224453/en-gb
		OBJECT: 7:34099162:0 
		KEY: 7:281474978938880 (f986588b1ade)
		*/
END

GO
EXEC sp_addextendedproperty @level0name='jt',@level0type='SCHEMA',@level1name='DeadlockGraphShredder',@level1type='PROCEDURE',@name='CodeLibraryDescription',@value='Shreds all deadlock XML graphs found in the XEvents ring buffer';

