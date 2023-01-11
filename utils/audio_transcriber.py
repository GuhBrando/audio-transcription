# Databricks notebook source
import speech_recognition as sr
import os
from pydub import AudioSegment
from pydub.playback import play
import numpy as np
filename = 'myfile.wav'

job_inputs = dbutils.notebook.entry_point.getCurrentBindings()
AUDIO_FILE = job_inputs["audio_source"]
r = sr.Recognizer()
    
with sr.AudioFile(AUDIO_FILE) as source:
    audio = r.record(source, duration = job_inputs["duration"])
    text = r.recognize_google(audio, language = 'en-IN', show_all = True, offset = job_inputs["offset"])
    print("Transcript: " + text["alternative"][0]["transcript"])
