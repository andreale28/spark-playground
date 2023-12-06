create database if not exists logcontent;

drop table if exists logcontent.bronze;
drop table if exists logcontent.silver;
drop table if exists logcontent.gold;
drop table if exists logcontent.rfm;

create table logcontent.bronze engine = DeltaLake('http://minio:9000/sparkplayground/delta/bronze', 'minio', 'minio123');
create table logcontent.silver engine = DeltaLake('http://minio:9000/sparkplayground/delta/silver', 'minio', 'minio123');
create table logcontent.gold engine = DeltaLake('http://minio:9000/sparkplayground/delta/gold', 'minio', 'minio123');
create table logcontent.rfm engine = DeltaLake('http://minio:9000/sparkplayground/delta/silver/rfm', 'minio', 'minio123');



select * from logcontent.rfm;
