CREATE VIEW jt.vwAllSessions
AS
select      s.session_id
,           s.login_time
,           s.host_name
,           s.login_name
,           s.status as session_status
,           r.status as request_status
,           s.cpu_time as session_cpu_time
,           s.reads as session_reads
,           s.writes as session_writes
,           s.logical_reads as session_logical_reads
,           c.num_reads as connection_num_reads
,           c.num_writes as connection_num_writes
,           c.last_read as connection_last_read
,           c.last_write as connection_last_write
,           r.request_id
,           r.command as request_command
,           r.open_transaction_count as request_open_transaction_count
,           r.open_resultset_count as request_open_resultset_count
,           r.total_elapsed_time as request_total_elapsed_time
,           r.row_count as request_row_count
from  sys.dm_exec_sessions s
left  outer join sys.dm_exec_connections c 
on          c.session_id = s.session_id
left  outer join sys.dm_exec_requests r 
on          r.session_id = s.session_id
GO
EXEC sp_addextendedproperty @level0name='jt',@level0type='SCHEMA',@level1name='vwAllSessions',@level1type='VIEW',@name='CodeLibraryDescription',@value='All current sessions, plus info from sys.dm_exec_requests';
