select * FROM {{source('preqin','tbluser_details')}}
WHERE __DELETED = 'FALSE' OR __DELETED IS NULL