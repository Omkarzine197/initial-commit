--Name : Postgre DDL Script
--Owner : SRI
--Date cre : 08102022
--Modified date : 
--For more info follow us on instagram


--TABLES
--COUNTRY
--CITY
--ADDRESS
--STAFF
--PLAN_POSTPAID
--PLAN_PREPAID
--COMPLAINT
--SUBSCRIBER


-- -----------------------------------------------------
-- Schema PROD
-- -----------------------------------------------------
--DROP DATABASE IF EXISTS `PROD` ;

-- -----------------------------------------------------
-- Schema PROD
-- -----------------------------------------------------
--CREATE DATABASE `CUST`;
--USE `PROD` ;

-- -----------------------------------------------------
-- Table country
-- -----------------------------------------------------

CREATE TABLE country (
  cn_id character varying(10) PRIMARY KEY,
  cn_name character varying(45));

-- -----------------------------------------------------
-- Table state
-- -----------------------------------------------------

--CREATE TABLE state (
  --st_id character varying(10) PRIMARY KEY,
  --st_name character varying(45),
  --cn_id character varying(10));


-- -----------------------------------------------------
-- Table city
-- -----------------------------------------------------
CREATE TABLE city (
  ct_id character varying(10) PRIMARY KEY,
  ct_name character varying(45),
  --st_id character varying(10));
  cn_id character varying(10));


-- -----------------------------------------------------
-- Table Address
-- -----------------------------------------------------
CREATE TABLE address (
  add_id character varying(10) PRIMARY KEY,
  Street character varying(45),
  ct_id character varying(10));

-- -----------------------------------------------------
-- Table plan_prepaid
-- -----------------------------------------------------
CREATE TABLE plan_prepaid (
  plan_id character varying(10) PRIMARY KEY,
  plan_desc character varying(45),
  amount NUMERIC(10));

-- -----------------------------------------------------
-- Table plan_postpaid
-- -----------------------------------------------------
CREATE TABLE plan_postpaid (
  plan_id character varying(10) PRIMARY KEY,
  plan_desc character varying(45),
  amount NUMERIC(10));

-- -----------------------------------------------------
-- Table Staff
-- -----------------------------------------------------
CREATE TABLE staff (
  staff_id NUMERIC(10) PRIMARY KEY,
  name character varying(45),
  mob NUMERIC(10),
  email character varying(45),
  sys_cre_date TIMESTAMP,
  sys_upd_date TIMESTAMP,
  active_flag CHAR(1),
  add_id character varying(10),
  prepaid_plan_id character varying(20),
  postpaid_plan_id character varying(20));


-- -----------------------------------------------------
-- Table Complaint
-- -----------------------------------------------------
CREATE TABLE complaint (
  cmp_id NUMERIC(10) PRIMARY KEY,
  sid NUMERIC(10),
  regarding character varying(45),
  descr character varying(45),
  status character varying(45),
  staff_id NUMERIC(10),
  sys_cre_date TIMESTAMP,
  sys_upd_date TIMESTAMP);


-- -----------------------------------------------------
-- Table Subscriber
-- -----------------------------------------------------
CREATE TABLE subscriber (
  sid NUMERIC(10) PRIMARY KEY,
  name character varying(45),
  mob NUMERIC(10),
  email character varying(45),
  add_id character varying(10),
 -- cmp_id NUMERIC(10),
  sys_cre_date TIMESTAMP,
  sys_upd_date TIMESTAMP,
  Active_flag CHAR(1),
  prepaid_plan_id character varying(20),
  postpaid_plan_id character varying(20));

commit;