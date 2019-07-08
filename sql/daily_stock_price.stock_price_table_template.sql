CREATE TABLE "{}"(
    date DATE primary key,
    open NUMERIC(12, 2),
    high NUMERIC(12, 2),
    low NUMERIC(12, 2),
    close NUMERIC(12, 2),
    volume BIGINT,
    transaction BIGINT,
    created TIMESTAMP not null default now()
);

// 判斷欄位所需的 byte 數
select pg_column_size(a)
from (values
 	(1234::numeric(10, 2))
) s(a)
;

// 查詢現有的 table list
SELECT
 tablename
FROM
 pg_catalog.pg_tables
WHERE
 schemaname != 'pg_catalog'
AND schemaname != 'information_schema';