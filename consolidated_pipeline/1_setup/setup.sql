-- Databricks notebook source

CREATE CATALOG IF NOT EXISTS fmcg;
USE CATALOG fmcg;

-- COMMAND ----------


CREATE SCHEMA IF NOT EXISTS fmcg.gold;
CREATE SCHEMA IF NOT EXISTS fmcg.silver;
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;