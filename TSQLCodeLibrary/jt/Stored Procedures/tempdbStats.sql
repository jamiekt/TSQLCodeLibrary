create proc jt.tempdbStats
AS
--from http://msdn.microsoft.com/en-us/library/ms176029.aspx
--Determining the Amount of Free Space in tempdb
SELECT SUM(unallocated_extent_page_count) AS [free pages], 
(SUM(unallocated_extent_page_count)*1.0/128) AS [free space in MB]
FROM tempdb.sys.dm_db_file_space_usage;
--Determining the Longest Running Transaction
SELECT transaction_id as longest_running_transction_id
FROM tempdb.sys.dm_tran_active_snapshot_database_transactions 
ORDER BY elapsed_time_seconds DESC;
--Determining the Amount Space Used by the Version Store
SELECT SUM(version_store_reserved_page_count) AS [version store pages used],
(SUM(version_store_reserved_page_count)*1.0/128) AS [version store space in MB]
FROM tempdb.sys.dm_db_file_space_usage;
--Determining the Amount of Space Used by Internal Objects
SELECT SUM(internal_object_reserved_page_count) AS [internal object pages used],
(SUM(internal_object_reserved_page_count)*1.0/128) AS [internal object space in MB]
FROM tempdb.sys.dm_db_file_space_usage;
--Determining the Amount of Space Used by User Objects
SELECT SUM(user_object_reserved_page_count) AS [user object pages used],
(SUM(user_object_reserved_page_count)*1.0/128) AS [user object space in MB]
FROM tempdb.sys.dm_db_file_space_usage;
--Determining the Total Amount of Space (Free and Used)
SELECT SUM(size)*1.0/128 AS [size in MB]
FROM tempdb.sys.database_files