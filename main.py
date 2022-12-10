# Databricks notebook source
STORAGE_ACCOUNT = "6c4d2a82-5766-48b1-a881-a901d58143a8"

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
# MAGIC apt-get install libav-tools libavcodec-extra

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade pip setuptools wheel

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install portaudio19-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install ffmpeg

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install SpeechRecognition

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install AudioConverter

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pipwin

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install youtube_dl

# COMMAND ----------

import speech_recognition as sr
import os
from pydub import AudioSegment
from pydub.playback import play
import numpy as np

sound_file = AudioSegment.from_mp3("My Childhood Obsession with Animals-6E94j0Goo8A.mp3")
sound_file_Value = np.array(sound_file.get_array_of_samples())
# milliseconds in the sound track
ranges = [(30000,1000000)] 

for x, y in ranges:
    new_file=sound_file_Value[x : y]
    song = AudioSegment(new_file.tobytes(), frame_rate=sound_file.frame_rate,sample_width=sound_file.sample_width,channels=1)
    song.export(str(x) + "-" + str(y) +".mp3", format="mp3")

# COMMAND ----------

import speech_recognition as sr
import os
from pydub import AudioSegment
from pydub.playback import play
import numpy as np
filename = 'myfile.wav'

AUDIO_FILE = "ytbvideo.wav"
r = sr.Recognizer()
    
with sr.AudioFile(AUDIO_FILE) as source:
    audio = r.record(source)
    text = r.recognize_google(audio, language = 'en-IN', show_all = True)
    print(text)
    print("Transcript: " + r.recognize_google(audio, language = 'en-IN', show_all = True))

# COMMAND ----------

from pydub import AudioSegment
sound = AudioSegment.from_mp3("30000-100000.mp3")
sound.export("ytbvideo.wav", format="wav")

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install pulseaudio-equalizer

# COMMAND ----------

import youtube_dl
from __future__ import unicode_literals

audio_downloader = {
    'format': 'bestaudio/best',
    'postprocessors': [{
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'mp3',
        'preferredquality': '192',
    }],
}

with youtube_dl.YoutubeDL(audio_downloader) as ydl:
    ydl.download(['https://www.youtube.com/watch?v=6E94j0Goo8A&ab_channel=JaidenAnimations'])


try:
    print('Youtube Downloader'.center(40, '_'))
    teste = audio_downloader.extract_info("https://www.youtube.com/watch?v=8BSYycuH-q8&ab_channel=GenshinImpact")
    print(teste)
except Exception:
    print("Couldn\'t download the audio")


# COMMAND ----------

spark.conf.set(
  f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
  ""
)

# COMMAND ----------

target_folder_path = 'abfss://audiotranscriptor@guhbrandaohotmail.dfs.core.windows.net/audiotranscriptor'

#write as parquet data
df_covid.write.format("parquet").save(target_folder_path)

# COMMAND ----------

# MAGIC %sh
# MAGIC pkill -f apt
