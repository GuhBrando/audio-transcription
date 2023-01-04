# Databricks notebook source
import speech_recognition as sr
import os
from pydub import AudioSegment
from pydub.playback import play
import numpy as np
filename = 'myfile.wav'

AUDIO_FILE = "ytbvideo.wav"
r = sr.Recognizer()
    
with sr.AudioFile(AUDIO_FILE) as source:
    audio = r.record(source, duration=120)
    text = r.recognize_google(audio, language = 'en-IN', show_all = True)
    print("Transcript: " + text["alternative"][0]["transcript"])
