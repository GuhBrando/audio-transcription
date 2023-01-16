# Databricks notebook source
import youtube_dl
import shutil
import os
import yt_dlp

# COMMAND ----------

def store_audio_file(origin_file, dest_location_plus_file):
    try:
        os.system(f'cp {origin_file} {dest_location_plus_file}')
        return origin_file
    except FileNotFoundError:
        origin_file = origin_file.replace("|", "_").replace("?", "")
        dest_location_plus_file = dest_location_plus_file.replace("|", "_").replace("?", "")
        shutil.copyfile(origin_file, dest_location_plus_file)
        return origin_file
      
def hook(d):
    if d['status'] == 'finished':
        filename = d['filename']
        print(filename)

def client(video_url, download=False):
    with yt_dlp.YoutubeDL(ydl_configs) as ydl:
         return ydl.extract_info(video_url, download=download)

# COMMAND ----------

job_inputs = dbutils.notebook.entry_point.getCurrentBindings()
video_id = job_inputs["video_id"]
video_url = job_inputs["video_link"]
print(video_url)

ydl_configs = {
        'format': 'bestaudio/best',
        'outtmpl': '%(id)s.%(ext)s',
        'download_archive': 'downloaded.txt',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'wav',
            'preferredquality': '192',
        }],
            'progress_hooks': [hook]
    }

try:
    video_details = client(video_url, download=False)
    video_download = client(video_url, download=True)
    print(video_details.get('title', None), video_details.get('id', None))
    video_id_ext = video_details["id"]+".wav"
    adb_workspace = "/Workspace/Repos/guh.brandao@hotmail.com/audio-transcription/utils/youtube_folder/"
    adls_path = "/dbfs/mnt/audio-transcription-files/"
    video_title = video_details.get('title', None)+"-"+video_details.get('id', None)+".wav"
except:
    os.remove(video_details["id"]+".webm")
