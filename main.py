# Databricks notebook source
import speech_recognition as sr
import os
from pydub import AudioSegment

AUDIO_FILE = "felipeNetoXingandoLula.wav"
r = sr.Recognizer()

with sr.AudioFile(AUDIO_FILE) as source:
    audio = r.record(source)
    print("Transcript: " + r.recognize_google(audio,language="pt-BR"))
