# Databricks notebook source
# MAGIC %run ./utils/config_and_setup

# COMMAND ----------

# MAGIC %run ./utils/DDL

# COMMAND ----------

# MAGIC %run ./utils/adls_manipulation

# COMMAND ----------

# MAGIC %run ./utils/video_downloader_factory

# COMMAND ----------

# MAGIC %run ./utils/audio_transcriber_starter

# COMMAND ----------

StartFactory.download_youtube_videos("Como jogar genshin impact", 5)
counter = 0
try:
    while len(requests.get(LIST_RUNNING_JOBS, json={"active_only": "true"}, headers={"Authorization":"Bearer "+DATABRICKS_AUTHORIZATION}).json()["runs"]) != 0 and counter <= 120:
        time.sleep(5)
        counter += 1
except KeyError:
    print("Finalizado")
requests.post(CANCEL_RUNNING_JOBS, json={"job_id": 608143620388634}, headers={"Authorization":"Bearer "+DATABRICKS_AUTHORIZATION})

# COMMAND ----------

# MAGIC %sh
# MAGIC cp ./utils/youtube_folder/*.wav /dbfs/mnt/audio-transcription-files/
# MAGIC rm ./utils/youtube_folder/*.wav
# MAGIC rm ./utils/youtube_folder/*.webm

# COMMAND ----------

transcript_audio_files()

# COMMAND ----------

# DBTITLE 1,Exemplo de anonimizacao - Exclusão de registro sem consentimento do usuário
# MAGIC %sql
# MAGIC insert into audt.audio_transcription values ('https://www.youtube.com/watch?v=JH3F9TyH7hgsdfe','','','1111','','2023-01-16')

# COMMAND ----------

videos_on_database = spark.sql("select video_url from audt.audio_transcription").collect()
unavailable_videos = []

for check_if_video_exists in videos_on_database:
    try:
        video = Video.getInfo(check_if_video_exists[0], mode = ResultMode.json)
    except TypeError:
        unavailable_videos.append(check_if_video_exists[0])

unavailable_videos_query = "'"+("', '".join(unavailable_videos))+"'"
print(unavailable_videos_query)
spark.sql(f"DELETE FROM audt.audio_transcription WHERE video_url IN ({unavailable_videos_query})")
