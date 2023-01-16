# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists audt

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS audt.audio_transcription(
video_url string,
social_network string,
video_title string,
views int,
transcription string,
date_time timestamp
)
USING DELTA
LOCATION "abfss://s-audio-transcription-files@staaudiotranscripter.dfs.core.windows.net/audt/audio_transcription";
""")
