# Databricks notebook source
STORAGE_ACCOUNT = "6c4d2a82-5766-48b1-a881-a901d58143a8"

# COMMAND ----------

# MAGIC %sh%sh
# MAGIC ls /local_disk0/.ephemeral_nfs/envs/pythonEnv-4cb87391-5a6d-4cfa-962c-cf2f8ad2fa0d/bin/python

# COMMAND ----------

dbutils.fs.put("/databricks/ffmpeg_install.sh", """ 
 
#! /bin/bash
# do a backup of the source 
cp /etc/apt/sources.list{,.bak}
 
# remove the cached mirrors
r="deb http://archive.ubuntu.com/ubuntu/ focal-updates main restricted"
add-apt-repository --remove "${r}"
r="deb http://archive.ubuntu.com/ubuntu/ focal-updates universe"
add-apt-repository --remove "${r}"
r="deb http://security.ubuntu.com/ubuntu/ focal-security main restricted"
add-apt-repository --remove "${r}"
 
# update apt & install the package
apt-get update
apt-get install -y ffmpeg
 
""", True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cp ffmpeg-2022-12-08-git-9ca139b2aa-full_build.7z /local_disk0/.ephemeral_nfs/envs/pythonEnv-4cb87391-5a6d-4cfa-962c-cf2f8ad2fa0d/bin/python/Scripts

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install SpeechRecognition

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install AudioConverter

# COMMAND ----------

import speech_recognition as sr
import os
from pydub import AudioSegment

AUDIO_FILE = "file.wav"
r = sr.Recognizer()

with sr.AudioFile(AUDIO_FILE) as source:
    audio = r.record(source)
    print("Transcript: " + r.recognize_google(audio,language="en-US"))

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install ffmpeg

# COMMAND ----------

from pydub import AudioSegment
sound = AudioSegment.from_mp3("TGA 2022 Genshin Impact Entry Video _ Genshin Impact-8BSYycuH-q8.mp3")
sound.export("file.wav", format="wav")

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install youtube_dl

# COMMAND ----------

import youtube_dl
from __future__ import unicode_literals

audio_downloader = {
    'format': 'bestaudio/best',
    'postprocessors': [{
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'wav',
        'preferredquality': '192',
    }],
}

with youtube_dl.YoutubeDL(audio_downloader) as ydl:
    ydl.download(['https://www.youtube.com/watch?v=8BSYycuH-q8&ab_channel=GenshinImpact'])


try:
    print('Youtube Downloader'.center(40, '_'))
    teste = audio_downloader.extract_info("https://www.youtube.com/watch?v=8BSYycuH-q8&ab_channel=GenshinImpact")
    print(teste)
except Exception:
    print("Couldn\'t download the audio")


# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ffmpeg 

# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
  ""
)

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------



# COMMAND ----------

target_folder_path = 'abfss://audiotranscriptor@guhbrandaohotmail.dfs.core.windows.net/audiotranscriptor'

#write as parquet data
df_covid.write.format("parquet").save(target_folder_path)
