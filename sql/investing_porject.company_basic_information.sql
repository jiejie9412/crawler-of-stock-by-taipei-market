 CREATE TABLE "company_basic_information" (
	serial_number integer not null,
	stock_type varchar(100) NOT null,
	stock_number varchar(100) not null,
	short_name varchar(300) not null,
	status varchar(100) NOT null,
	remark varchar(3000),
	industry_category  varchar(100),
	full_name varchar(1000),
	establishment_date date,
	listing_date_otcm date,
	listing_date_em date,
	delisting_date date,
	updated TIMESTAMP not null default current_timestamp,
	created TIMESTAMP not null default now()	
);
