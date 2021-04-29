select * FROM {{source('preqin','tblFirm_Type')}}
--WHERE __DELETED = 'FALSE' OR __DELETED IS NULL