
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


create table metadata.data_type_mapping
(
	source_type varchar(50),
	source_data_type varchar(50),
	source_data_type_length integer, 
	anchor_data_type varchar(50),
	anchor_data_type_length integer
);

INSERT INTO metadata.data_type_mapping (source_type,source_data_type,source_data_type_length,anchor_data_type,anchor_data_type_length) VALUES 
('MSSQL','BLOB',NULL,'VARBINARY',NULL)
,('MSSQL','BIGINT',NULL,'BIGINT',NULL)
,('MSSQL','BINARY',NULL,'BYTEA',NULL)
,('MSSQL','BIT',NULL,'BOOLEAN',NULL)
,('MSSQL','CHAR',NULL,'CHAR',NULL)
,('MSSQL','DATE',NULL,'DATE',NULL)
,('MSSQL','DATETIME',NULL,'TIMESTAMP',NULL)
,('MSSQL','DECIMAL',NULL,'DECIMAL',NULL)
,('MSSQL','DOUBLE PRECISION',NULL,'DOUBLE PRECISION',NULL)
,('MSSQL','FLOAT',NULL,'FLOAT',NULL)
,('MSSQL','IMAGE',NULL,'BYTEA',NULL)
,('MSSQL','INT',NULL,'INT',NULL)
,('MSSQL','MONEY',NULL,'MONEY',NULL)
,('MSSQL','NCHAR',NULL,'CHAR',NULL)
,('MSSQL','NTEXT',NULL,'TEXT',NULL)
,('MSSQL','NUMERIC',NULL,'NUMERIC',NULL)
,('MSSQL','NVARCHAR',NULL,'VARCHAR',NULL)
,('MSSQL','REAL',NULL,'REAL',NULL)
,('MSSQL','ROWVERSION',NULL,'BYTEA',NULL)
,('MSSQL','SMALLDATETIME',NULL,'TIMESTAMP',0)
,('MSSQL','SMALLINT',NULL,'SMALLINT',NULL)
,('MSSQL','SMALLMONEY',NULL,'MONEY',NULL)
,('MSSQL','TEXT',NULL,'TEXT',NULL)
,('MSSQL','TIME',NULL,'TIME',NULL)
,('MSSQL','TIMESTAMP',NULL,'BYTEA',NULL)
,('MSSQL','TINYINT',NULL,'SMALLINT',NULL)
,('MSSQL','UNIQUEIDENTIFIER',NULL,'CHAR',16)
,('MSSQL','VARCHAR',NULL,'VARCHAR',NULL)
,('MSSQL','XML',NULL,'XML',NULL)
,('MSSQL','NVARCHAR',-1,'TEXT',NULL)
,('MSSQL','VARCHAR',-1,'TEXT',NULL);

create table metadata.table 
(
	table_id int not null,
	disable_flg int not null	
);

create table metadata.table_attr
(
	table_id int not null, 
	attr_type varchar(100) not null, 
	attr_value varchar(1000) not null
);

insert into metadata.attr_type
(attr_type, attr_desc, object_nm)
values 
('table_name','Наименование таблицы','table')
,('table_type','Тип таблицы','table')
,('schema','Схема таблицы','table')
,('source_table_name','Наименование таблицы на источнике','table');


create table metadata.source_table
(
	table_id int not null, 
	source_id int not null
);

create table metadata.column
(
	column_id int not null, 
	disable_flg int not null
);

create table metadata.column_attr
(
	column_id int not null, 
	attr_type varchar(100) not null, 
	attr_value varchar(1000) not null
)

insert into metadata.attr_type
(attr_type, attr_desc, object_nm)
values 
('column_name','Наименование атрибута','column')
,('column_type','Тип атрибута','column')
,('datatype','Тип данных','column')
,('length','Длина атрибута','column')
,('scale','Количество знаков после запятой','column')
,('source_column_name','Наименование атрибута на источнике','column')
