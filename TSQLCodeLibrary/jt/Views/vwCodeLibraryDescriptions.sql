CREATE VIEW [jt].[vwCodeLibraryDescriptions]
	AS
SELECT	schema_name=object_schema_name(major_id)
,		object_name=object_name(major_id)
,		CodeLibraryDescription=value
,		object_type=o.type_desc
FROM	sys.extended_properties ep
INNER JOIN sys.objects o
	ON	ep.major_id = o.object_id
WHERE	ep.name = N'CodeLibraryDescription'

