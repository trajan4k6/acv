

select * FROM {{source('preqin','tbluser_Subscription')}}
WHERE __DELETED = 'FALSE' OR __DELETED IS NULL