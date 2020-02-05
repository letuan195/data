create database vn30;
use vn30;

show tables;
select * from security where name='vci';
select * from free_float_real where date = '2019-07-15' and type='VN30';
select * from free_float_day;
select * from daily_data where sec_id=1571 order by date desc;
select * from daily_data where sec_id=1571 and date >= '2018-01-01' and date<= '2018-12-31' order by date desc;
delete from daily_data where date='2020-01-31';


delete from free_float_day where date = '2020-07-15';

describe security;
CREATE TABLE security (
  id bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'Id tự sinh',
  name varchar(100) DEFAULT NULL COMMENT 'Mã giao dịch',
  full_name varchar(250) DEFAULT NULL,
  date_of_listing datetime DEFAULT NULL COMMENT 'ngay niem yet',
  initial_listing_price decimal(20, 0) DEFAULT NULL COMMENT 'gia chao san',
  charter_capital decimal(20, 0) DEFAULT NULL COMMENT 'von dieu le',
  listing_volume decimal(20, 0) DEFAULT NULL COMMENT 'khoi luong niem yet',
  last_updated datetime DEFAULT NULL COMMENT 'ngay cap nhat',
  exchange varchar(100) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE INDEX id_idx (id)
)
ENGINE = INNODB,
CHARACTER SET utf8,
COLLATE utf8_general_ci,
COMMENT = 'Danh sách mã giao dịch';

drop table daily_data;

describe daily_data;
create table daily_data (
	id bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'Id tự sinh',
	sec_id bigint(20) NOT NULL COMMENT 'id co phieu',
    date date DEFAULT NULL COMMENT 'ngay giao dich',
    market_cap decimal(20, 0) DEFAULT NULL COMMENT 'von hoa thi truong',
    shares decimal(20, 0) DEFAULT NULL COMMENT 'so luong co phieu luu hanh',
    close decimal(20, 0) DEFAULT NULL COMMENT 'gia dong cua',
    trade_value decimal(20, 0) DEFAULT NULL COMMENT 'gia tri giao dich',
    free_shares decimal(20, 0) DEFAULT NULL COMMENT 'so luong co phieu tu do',
    
    PRIMARY KEY (id),
	foreign key(sec_id) references security(id)
);

drop table free_float_day;
create table free_float_day (
	id bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'Id tự sinh',
	stt int NOT NULL COMMENT 'so thu tu',
	sec_id bigint(20) NOT NULL COMMENT 'id co phieu',
    date date DEFAULT NULL COMMENT 'ngay giao dich',
    free_float double DEFAULT NULL COMMENT 'free float',
    free_float_adj double DEFAULT NULL COMMENT 'free float lam tron',
    shares decimal(20, 0) DEFAULT NULL COMMENT 'so luong co phieu luu hanh',
    weight_market_cap double DEFAULT NULL COMMENT 'gioi han ty trong von hoa',
    type varchar(50) DEFAULT NULL COMMENT 'loai chi so',
    PRIMARY KEY (id),
	foreign key(sec_id) references security(id)
);

drop table free_float_real;
create table free_float_real (
	id bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'Id tự sinh',
	stt int NOT NULL COMMENT 'so thu tu',
	sec_id bigint(20) NOT NULL COMMENT 'id co phieu',
    date date DEFAULT NULL COMMENT 'ngay giao dich',
    free_float double DEFAULT NULL COMMENT 'free float',
    free_float_adj double DEFAULT NULL COMMENT 'free float lam tron',
    shares decimal(20, 0) DEFAULT NULL COMMENT 'so luong co phieu luu hanh',
    weight_market_cap double DEFAULT NULL COMMENT 'gioi han ty trong von hoa',
    type varchar(50) DEFAULT NULL COMMENT 'loai chi so',
    PRIMARY KEY (id),
	foreign key(sec_id) references security(id)
);

alter table security
add exchange varchar(100)