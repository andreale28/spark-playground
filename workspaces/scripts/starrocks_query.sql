CREATE EXTERNAL CATALOG deltalake_catalog_hms
PROPERTIES
(
    "type" = "deltalake",
    "hive.metastore.type" = "hive",
    "hive.metastore.uris" = "thrift://metastore:9083",
    "aws.s3.enable_ssl" = "true",
    "aws.s3.enable_path_style_access" = "true",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "minio",
    "aws.s3.secret_key" = "minio123"
);
show catalogs;

set catalog deltalake_catalog_hms;
show databases from deltalake_catalog_hms;
use deltalake_catalog_hms.logcontent;
desc deltalake_catalog_hms.logcontent.gold;

with rfm_temp as
    (select
        Contract
        , concat(FrequencyScore, FrequencyScore, MonetaryDurationScore) as rfm_score
    from deltalake_catalog_hms.logcontent.gold
)

select
    Contract
    , rfm_score
from rfm_temp
where rfm_score not like '111';

