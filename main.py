# Databricks notebook source
# MAGIC %run ./utils/config_and_setup

# COMMAND ----------

# MAGIC %run ./utils/DDL

# COMMAND ----------

# MAGIC %run ./utils/adls_manipulation

# COMMAND ----------

# MAGIC %run ./utils/video_downloader_factory

# COMMAND ----------

def download_youtube_videos(youtube_video_search, quantity_of_videos):
    try:
        with open(r'/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription/utils/youtube_folder/downloaded.txt') as f:
            already_downloaded_videos = [row[1] for row in csv.reader(f,delimiter=' ')]
    except FileNotFoundError:
        already_downloaded_videos = []
    videos_search = VideosSearch(youtube_video_search, limit = 100)
    videos_infos = []
    for video in videos_search.result()["result"]:
        if len(videos_infos) == quantity_of_videos:
            break
        else:
            try:
                if len(video["duration"].split(":")) <= 2 and int(video["duration"].split(":")[0]) <= 15 and int(video["duration"].split(":")[0]) >= 3 and video["id"] not in already_downloaded_videos:
                    videos_infos.append(video)
            except:
                pass
    video_downloader_factory("youtube", videos_infos)

# COMMAND ----------

def transcript_audio_files():
    audio_files = os.listdir("/dbfs/mnt/audio-transcription-files")
    r = sr.Recognizer()
    for file in audio_files:
        api_payload = {
                "job_id": AUDIO_TRANSCRIBER_JOB_ID,
                "notebook_params": {
                    "audio_source": str('/dbfs/mnt/audio-transcription-files/'+file),
                }
            }
        jobs_audio_transcriber = requests.post(RUN_JOB_API, json=api_payload, headers={"Authorization":"Bearer "+DATABRICKS_AUTHORIZATION})
        print(jobs_audio_transcriber.json())

# COMMAND ----------


download_youtube_videos("Santander Brasil Opiniões", 3)
try:
    while len(requests.get(LIST_RUNNING_JOBS, json=api_payload, headers={"Authorization":"Bearer "+DATABRICKS_AUTHORIZATION}).json()["runs"]) != 0:
        time.sleep(5)
except KeyError:
    print("Finalizado")

# COMMAND ----------

# MAGIC %sh
# MAGIC cp ./utils/youtube_folder/*.wav /dbfs/mnt/audio-transcription-files/
# MAGIC rm ./utils/youtube_folder/*.wav

# COMMAND ----------

transcript_audio_files()

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

account_url = "https://staaudiotranscripter.dfs.core.windows.net"
default_credential = DefaultAzureCredential()

upload_file_path = os.path.join("/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription", "ytbvideo.wav")
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

os.listdir("/dbfs/mnt/audio-transcription-files")[0]

# COMMAND ----------

from pydub import AudioSegment

#shutil.copyfile("/dbfs/mnt/audio-transcription-files/"+os.listdir("/dbfs/mnt/audio-transcription-files")[2], os.listdir("/dbfs/mnt/audio-transcription-files")[2])
#file = "/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription/AUMENTOS DE LIMITES BANCO DO BRASIL - SANTANDER E RESPONDENDO COMENTARIOS-91vI9AVIIGE.mp3"
sound = AudioSegment.from_mp3("/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription/AUMENTOS DE LIMITES BANCO DO BRASIL - SANTANDER E RESPONDENDO COMENTARIOS-91vI9AVIIGE.mp3")
sound.export("/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription/AUMENTOS DE LIMITES BANCO DO BRASIL - SANTANDER E RESPONDENDO COMENTARIOS123-91vI9AVIIGE.wav", format="wav")

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get install ffmpeg

# COMMAND ----------

from os import path
from pydub import AudioSegment

# files                                                                         
src = "9AVIIGE.mp3"
dst = "test.wav"

# convert wav to mp3
AudioSegment.ffmpeg = "/usr/bin/ffmpeg"
sound = AudioSegment.from_mp3(src)
sound.export(dst, format="wav")

# COMMAND ----------

# MAGIC %sh
# MAGIC whereis ffmpeg

# COMMAND ----------

import subprocess
subprocess.call(['ffmpeg', '-i', 'AUMENTOS DE LIMITES BANCO DO BRASIL SANTANDER E RESPONDENDO COMENTARIOS91vI9AVIIGE.mp3',
                   'AUMENTOS DE LIMITES BANCO DO BRASIL - SANTANDER E RESPONDENDO COMENTARIOS123-91vI9AVIIGE.wav'])

# COMMAND ----------

os.listdir()
os.listdir("/dbfs/mnt/audio-transcription-files")

# COMMAND ----------

import subprocess

subprocess.call(['ffmpeg', '-i', '9AVIIGE.mp3',
               'porfavor2.wav'])

# COMMAND ----------

target_folder_path = 'abfss://audiotranscriptor@guhbrandaohotmail.dfs.core.windows.net/audiotranscriptor'

#write as parquet data
df_covid.write.format("parquet").save(target_folder_path)
