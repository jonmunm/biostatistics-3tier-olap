CREATE SCHEMA IF NOT EXISTS biostatistics;

CREATE TABLE biostatistics.dim_cause_of_death (
	id INTEGER,
	-- Subcategoria --
	subcategory_id INTEGER,
	subcategory_code VARCHAR,
	subcategory_name VARCHAR,
	-- Categoria --
	category_id INTEGER,
	category_code VARCHAR,
	category_name VARCHAR,
	-- Grupo 3 --
	group3_id INTEGER,
	group3_code VARCHAR,
	group3_name VARCHAR,
	-- grupo 2 --
	group2_id INTEGER,
	group2_code VARCHAR,
	group2_name VARCHAR,
	-- Grupo 1 --
	group1_id INTEGER,
	group1_code VARCHAR,
	group1_name VARCHAR,
	-- Capitulo --
	chapter_id INTEGER,
	chapter_code VARCHAR,
	chapter_name VARCHAR,
	chapter_number VARCHAR
);

CREATE TABLE biostatistics.dim_city
(
	id INTEGER,
	city_id INTEGER,
	city_cut INTEGER,
	city_name VARCHAR,
	-- Provincia
	province_id INTEGER,
	province_cut INTEGER,
	province_name VARCHAR,
	-- Servicio de salud
	healthcare_service_id INTEGER,
	healthcare_service_cut INTEGER,
	healthcare_service_name VARCHAR,
	-- Region
	region_id INTEGER,
	region_cut INTEGER,
	region_name VARCHAR,
	-- Validez
	valid_from INTEGER,
	valid_to INTEGER
);

CREATE TABLE biostatistics.dim_healthcare_service
(
	id INTEGER,
	healthcare_service_id INTEGER,
	healthcare_service_cut INTEGER,
	healthcare_service_name VARCHAR,
	-- Region
	region_id INTEGER,
	region_cut INTEGER,
	region_name VARCHAR,
	-- Validez
	valid_from INTEGER,
	valid_to INTEGER
);

CREATE TABLE biostatistics.dim_year 
(
	id INTEGER,
	year_name VARCHAR
);

CREATE TABLE biostatistics.dim_gender 
(
	id INTEGER,
	gender_name VARCHAR,
	gender_shortname VARCHAR
);

CREATE TABLE biostatistics.dim_age
(
	id INTEGER,
	-- Años
	age_id INTEGER,
	age_name VARCHAR,
	-- Por quinquenio de años
	fiveyear_span_id INTEGER,
	fiveyear_span_name VARCHAR,
	-- Por ciclo vital
	years_group_id INTEGER,
	years_group_name VARCHAR,
	-- Por quinquenio de años, alternativo
	alternative_fiveyear_span_id INTEGER,
	alternative_fiveyear_span_name VARCHAR,
	-- Por ciclo vital, alternativo
	alternative_years_group_id INTEGER,
	alternative_years_group_name VARCHAR,
	-- Decenio de años
	tenyear_span_id INTEGER,
	tenyear_span_name VARCHAR
);

CREATE TABLE biostatistics.dim_health_insurance (
	id INTEGER,
	health_insurance_code INTEGER,
	health_insurance_id INTEGER,
	health_insurance_name VARCHAR,
	health_insurance_updated_name VARCHAR,
	valid_from INTEGER,
	valid_to INTEGER
);

CREATE TABLE biostatistics.fact_biostatistics
(
	health_insurance_id INTEGER,
	year_id INTEGER,
	gender_id INTEGER,
	age_id INTEGER,
	city_id INTEGER,
	healthcare_service_id INTEGER,
	main_cause_of_death_id INTEGER,
	external_cause_of_death_id INTEGER
);
