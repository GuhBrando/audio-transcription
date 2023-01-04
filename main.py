# Databricks notebook source
password = dbutils.secrets.get(scope="akv-audio-transcription", key="storage-app-password")

# COMMAND ----------

print(password)

# COMMAND ----------

# MAGIC %run ./utils/config_and_setup

# COMMAND ----------

# MAGIC %run ./utils/config_and_setup

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

def mount_disk(fileSystemName):
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": appID,
        "fs.azure.account.oauth2.client.secret": password,
        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+tenantID+"/oauth2/token",
        "fs.azure.createRemoteFileSystemDuringInitialization": "true"
    }

    dbutils.fs.mount(
    source = "abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/",
    mount_point = "/mnt/"+fileSystemName,
    extra_configs = configs
    )

fileSystemName = "audio-transcription-files"
try:
    mount_disk(fileSystemName)
except:
    print("WARN - Mount do File System ja foi executado. | Skipped")

# COMMAND ----------

def store_audio_file(origin_file, dest_location_plus_file):
    try:
        shutil.copyfile(origin_file, dest_location_plus_file)
        return origin_file
    except FileNotFoundError:
        origin_file = origin_file.replace("|", "_").replace("?", "")
        dest_location_plus_file = dest_location_plus_file.replace("|", "_").replace("?", "")
        shutil.copyfile(origin_file, dest_location_plus_file)
        return origin_file

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://github.com/mozilla/geckodriver/releases/download/v0.32.0/geckodriver-v0.32.0-linux64.tar.gz -O /tmp/geckodriver.tar.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC tar -xvzf /tmp/geckodriver.tar.gz -C /tmp

# COMMAND ----------

# MAGIC %sh
# MAGIC /usr/bin/yes | sudo apt update --fix-missing > /dev/null 2>&1

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get --yes --force-yes install firefox > /dev/null 2>&1

# COMMAND ----------

def download_youtube_videos(youtube_video_search, video_quantity):
    videosSearch = CustomSearch(youtube_video_search, VideoSortOrder.uploadDate, limit = video_quantity, language='pt', region = 'BR')
    for video in videosSearch.result()["result"]:
        print(video)
        video_title = video["title"]+"-"+video["id"]+".mp3"
        audio_downloader = {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'mp3',
                'preferredquality': '192',
            }]
        }

        with youtube_dl.YoutubeDL(audio_downloader) as ydl:
            result = ydl.extract_info(
                        video["link"],
                        download=True
                    )
        print(video["title"])
        adb_workspace = "/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription/"
        adls_path = "/dbfs/mnt/audio-transcription-files/"
        audio_path = store_audio_file(adb_workspace+video_title, adls_path+video_title)
        os.remove(audio_path)
download_youtube_videos("Minecraft", 2)

# COMMAND ----------

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver.get("https://www.google.com")

# COMMAND ----------

options = Options()
options.headless = True
driver = webdriver.Firefox(options=options, executable_path='/tmp/geckodriver')
driver.implicitly_wait(5)

search_query = input().split()
print(search_query)

for word in search_query:
    final_query += word + "+"
    
driver.get('https://www.youtube.com/results?search_query={}'.format(final_query))
select = driver.find_element(By.CSS_SELECTOR, 'div#contents ytd-item-section-renderer>div#contents a#thumbnail')
link += [select.get_attribute('href')]
print(link)

# COMMAND ----------

AUDIO_FILE = "blackhole.wav"
r = sr.Recognizer()
    
with sr.AudioFile(AUDIO_FILE) as source:
    audio = r.record(source,offset=30, duration=30)
    text = r.recognize_google(audio, language = 'en-IN', show_all = True)
    print(text)
    print("Transcript: " + text["alternative"][0]["transcript"])

# COMMAND ----------

account_url = "https://staaudiotranscripter.dfs.core.windows.net"
default_credential = DefaultAzureCredential()

upload_file_path = os.path.join("/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription", "ytbvideo.wav")
# Create the BlobServiceClient object
blob_service_client = BlobServiceClient(account_url, credential=default_credential)
print(blob_service_client)

local_file_name = str(uuid.uuid4()) + ".wav"

blob_client = blob_service_client.get_blob_client(container="audiotranscriptor", blob="/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription/ytbvideo.wav")

print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

# Upload the created file
with open(file=upload_file_path, mode="rb") as data:
    blob_client.upload_blob(data)

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

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

AUDIO_FILE = "blackhole.wav"
r = sr.Recognizer()
    
with sr.AudioFile(AUDIO_FILE) as source:
    audio = r.record(source,offset=30, duration=30)
    text = r.recognize_google(audio, language = 'en-IN', show_all = True)
    print(text)
    print("Transcript: " + text["alternative"][0]["transcript"])

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install pulseaudio-equalizer

# COMMAND ----------

import youtube_dl
from __future__ import unicode_literals
SAVE_PATH = "/dbfs/mnt/data/"


audio_downloader = {
    'format': 'bestaudio/best',
    'postprocessors': [{
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'mp3',
        'preferredquality': '192',
    }],
    'outtmpl':SAVE_PATH + '/%(title)s.%(ext)s',
}

with youtube_dl.YoutubeDL(audio_downloader) as ydl:
    ydl.download(['https://www.youtube.com/watch?v=aeWyp2vXxqA&ab_channel=Kurzgesagt%E2%80%93InaNutshell'])


# COMMAND ----------

from pydub import AudioSegment
sound = AudioSegment.from_mp3("Black Hole Star – The Star That Shouldn't Exist-aeWyp2vXxqA.mp3")
sound.export("blackhole.wav", format="wav")

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
