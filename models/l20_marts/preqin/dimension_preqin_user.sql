
with dimension_maintenance_user as (
SELECT 
{{ dbt_utils.surrogate_key(
      'user_id'
  ) }} as DIMENSION_maintenance_user_KEY,
MU.user_id as PreqinUserId,
substring(MU.user_fullname,1,CHARINDEX(' ', MU.user_fullname)) as FirstName,
substring(MU.user_fullname,CHARINDEX(' ', MU.user_fullname),LEN(user_fullname)) as LastName,
MU.user_fullname as FullName,
MU.user_initials as UserInitials,
MU.WindowsUserName as WindowsUserName,

to_char(add_months(MU.user_created_date, -1), 'YYYYMMDD') as UserCreatedDateKey,

--cast(convert(varchar(8), MU.user_created_date, 112) as int) as UserCreatedDateKey,
--cast(convert(varchar(8), MU.user_password_expiry_date, 112) as int) as UserPasswordExpiryDateKey,
MU.user_superuser as IsSuperuser,
MU.user_delete_Permission as HasDeletePermission,
MU.Active as IsActive
FROM DB_RAW.PREQIN01_FIVETRAN_DBO.TBLMAINTENANCEUSERS MU

)


select * from dimension_maintenance_user