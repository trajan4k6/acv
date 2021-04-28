select * FROM {{source('preqin','tblContact')}}
WHERE __DELETED = 'FALSE' OR __DELETED IS NULL