create view jt.vwCacheUsagePerTable
AS
/*
Taken from a blog by Steve Hindmarsh at http://sqlblogcasts.com/blogs/steveh/archive/2010/04/02/dbcc-memusage-in-2008.aspx

*/
WITH memusage_CTE AS (
	SELECT	bd.database_id, bd.file_id, bd.page_id, bd.page_type 
	,		COALESCE(p1.object_id, p2.object_id) AS object_id 
	,		COALESCE(p1.index_id, p2.index_id) AS index_id 
	,		bd.row_count
	,		bd.free_space_in_bytes
	,		CONVERT(TINYINT,bd.is_modified) AS 'DirtyPage' 
	FROM	sys.dm_os_buffer_descriptors AS bd 
	JOIN	sys.allocation_units AS au 
	ON		au.allocation_unit_id = bd.allocation_unit_id 
	OUTER	APPLY ( 
			SELECT	TOP(1) p.object_id, p.index_id 
			FROM	sys.partitions AS p 
			WHERE	p.hobt_id = au.container_id AND au.type IN (1, 3) 
			) AS p1 
	OUTER	APPLY ( 
			SELECT	TOP(1) p.object_id, p.index_id 
			FROM	sys.partitions AS p 
			WHERE	p.partition_id = au.container_id AND au.type = 2 
			) AS p2 
	WHERE	bd.database_id = DB_ID() 
	AND		bd.page_type IN ('DATA_PAGE', 'INDEX_PAGE') 
) 
SELECT	TOP 20 DB_NAME(database_id) AS 'Database'
,		OBJECT_NAME(object_id,database_id) AS 'Table Name'
,		index_id,COUNT(*) AS 'Pages in Cache'
,		SUM(dirtyPage) AS 'Dirty Pages' 
FROM	memusage_CTE 
GROUP	BY database_id, object_id, index_id 
ORDER	BY COUNT(*) DESC