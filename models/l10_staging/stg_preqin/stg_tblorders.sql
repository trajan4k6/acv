select *
FROM {{ source('preqin', 'tblorders') }}
WHERE __DELETED = 'FALSE' OR __DELETED IS NULL