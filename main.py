# Databricks notebook source
# MAGIC %run ./utils/config_and_setup

# COMMAND ----------

# MAGIC %run ./utils/DDL

# COMMAND ----------

# MAGIC %run ./utils/adls_manipulation

# COMMAND ----------

# MAGIC %run ./utils/video_downloader_factory

# COMMAND ----------

class startFactory:
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
    def download_tiktok_videos(tiktok_video_search, quantity_of_videos):
        #Tratamento
        video_downloader_factory("tiktok", videos_infos)

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

startFactory.download_youtube_videos("Como cuidar de gatos", 3)
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
