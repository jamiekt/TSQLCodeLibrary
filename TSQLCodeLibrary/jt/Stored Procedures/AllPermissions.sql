CREATE PROC jt.AllPermissions 
	@dbName sysname
AS
/*
Originally published here: http://blogs.conchango.com/jamiethomson/archive/2007/02/09/SQL-Server-2005_3A00_-View-all-permissions--_2800_2_2900_.aspx

This script will show all permissions that a user has in the current database, including those inherited from role memberships

Might be a good idea to create this as a view

This only works on SQL Server 2005 and later (because it uses a common table expression)

-Jamie Thomson
2008-11-08
*/
DECLARE @SQL nvarchar(max) = '
WITH    perms_cte as
(
        select USER_NAME(p.grantee_principal_id) AS principal_name,
                dp.principal_id,
                dp.type_desc AS principal_type_desc,
                p.class_desc,
                OBJECT_NAME(p.major_id) AS object_name,
                p.permission_name,
                p.state_desc AS permission_state_desc 
        from    [@dbName].sys.database_permissions p
        inner   JOIN [@dbName].sys.database_principals dp
        on     p.grantee_principal_id = dp.principal_id
)
--users
SELECT p.principal_name,  p.principal_type_desc, p.class_desc, p.[object_name], p.permission_name, p.permission_state_desc, cast(NULL as sysname) as role_name
FROM    perms_cte p
WHERE   principal_type_desc <> ''DATABASE_ROLE''
UNION
--role members
SELECT rm.member_principal_name, rm.principal_type_desc, p.class_desc, p.object_name, p.permission_name, p.permission_state_desc,rm.role_name
FROM    perms_cte p
right outer JOIN (
    select role_principal_id, dp.type_desc as principal_type_desc, member_principal_id,user_name(member_principal_id) as member_principal_name,user_name(role_principal_id) as role_name--,*
    from    [@dbName].sys.database_role_members rm
    INNER   JOIN [@dbName].sys.database_principals dp
    ON     rm.member_principal_id = dp.principal_id
) rm
ON     rm.role_principal_id = p.principal_id
order by 1';
SET @SQL = rePLACE(@SQL,'@dbName',@dbName)
EXEC (@SQL)

GO
EXEC sp_addextendedproperty @level0name='jt',@level0type='SCHEMA',@level1name='AllPermissions',@level1type='PROCEDURE',@name='CodeLibraryDescription',@value='All permissions granted to any user, either directly or via role membership.';
