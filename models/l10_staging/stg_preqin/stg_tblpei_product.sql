select * FROM {{source('preqin','tblpei_product')}}
WHERE __DELETED = 'FALSE' OR __DELETED IS NULL