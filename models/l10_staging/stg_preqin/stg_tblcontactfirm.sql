select * FROM {{source('preqin','tblContactFirm')}}
WHERE __DELETED = 'FALSE' OR __DELETED IS NULL