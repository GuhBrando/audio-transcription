# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists audt

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists g_audt

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

# COMMAND ----------

spark.sql(f"""
CREATE VIEW IF NOT EXISTS g_audt.videos_with_most_views AS
SELECT *
FROM audt.audio_transcription
WHERE views > 100000;
""")
