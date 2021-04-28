select * FROM {{source('preqin','tblFirm')}}
WHERE __DELETED = 'FALSE' OR __DELETED IS NULL