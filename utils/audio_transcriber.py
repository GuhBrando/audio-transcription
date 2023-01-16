# Databricks notebook source
import speech_recognition as sr
import os
from youtubesearchpython import Video, ResultMode
from pydub import AudioSegment
from pydub.playback import play

# COMMAND ----------

job_inputs = dbutils.notebook.entry_point.getCurrentBindings()
audio_source = job_inputs["audio_source"]
r = sr.Recognizer()

with sr.AudioFile(audio_source) as source:
    audio = r.record(source, duration = 180)
    transcription_text = r.recognize_google(audio, language = 'pt-BR', show_all = True)
    print(transcription_text)
    transcription_text = transcription_text["alternative"][0]["transcript"]
file = audio_source.split("/")[4].split(".")[0]
video = Video.getInfo("https://www.youtube.com/watch?v="+file, mode = ResultMode.json)
video_url = "https://www.youtube.com/watch?v="+file
video_title = video["title"]
views = video["viewCount"]["text"]
upload_date = video["publishDate"]

spark.sql(f"insert into audt.audio_transcription values ('{video_url}','Youtube','{video_title}','{views}','{transcription_text}','{upload_date}')")

os.remove(audio_source)
