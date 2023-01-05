# Databricks notebook source
#pip install -q selenium youtube-search-python youtube_dl azure.identity azure.storage.blob pipwin

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install -q --upgrade pip setuptools wheel

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install libav-tools libavcodec-extra

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install portaudio19-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install ffmpeg

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install pulseaudio-equalizer

# COMMAND ----------



# COMMAND ----------

from youtubesearchpython import *
from youtubesearchpython import VideosSearch
import youtube_dl
from __future__ import unicode_literals
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
#from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
import os
import speech_recognition as sr
from pydub import AudioSegment
from pydub.playback import play
import numpy as np
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import uuid
import os
import speech_recognition as sr
import os
from pydub import AudioSegment
from pydub.playback import play
import numpy as np
from pydub import AudioSegment
import youtube_dl
from __future__ import unicode_literals

STORAGE_ACCOUNT = "6c4d2a82-5766-48b1-a881-a901d58143a8"

spn_id = "64e72c3a-2237-4645-9e29-c7af44dc446a"
tenant_id = "02589359-ab33-4ac8-a14d-396cc39943ae"
spn_password = dbutils.secrets.get(scope="akv-audio-transcription", key="storage-app-password")
storageAccountName = "staaudiotranscripter"

spark.conf.set("fs.azure.account.auth.type.staaudiotranscripter.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.staaudiotranscripter.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.staaudiotranscripter.dfs.core.windows.net", spn_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.staaudiotranscripter.dfs.core.windows.net", spn_password)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.staaudiotranscripter.dfs.core.windows.net", "https://login.microsoftonline.com/02589359-ab33-4ac8-a14d-396cc39943ae/oauth2/token")
