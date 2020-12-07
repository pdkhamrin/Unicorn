
create database anchorbi;

create schema metadata;

CREATE TABLE metadata.attr_type (
	attr_type varchar(100) NOT NULL,
	attr_desc varchar(500) NOT NULL,
	object_nm varchar(50) NOT NULL
);

INSERT INTO metadata.attr_type (attr_type,attr_desc,object_nm) VALUES 
('source_name','Имя источника','source')
,('source_type','Тип источника','source')
,('server_name','Имя сервера','source')
,('port_number','Номер порта','source')
,('user','Пользователь','source')
,('password','Пароль','source')
,('database','База данных','source')
;

CREATE TABLE metadata."source" (
	source_id int4 NOT NULL,
	disable_flg bit(1) NULL,
	CONSTRAINT source_pkey PRIMARY KEY (source_id)
);

CREATE TABLE metadata.source_attr (
	source_id serial NOT NULL,
	attr_type varchar(100) NULL,
	attr_value varchar(1000) NULL
);
