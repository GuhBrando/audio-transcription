# Databricks notebook source
def youtube_video_downloader(values):
    for video in values:
        print(video["title"])
        api_payload = {
            "job_id": YOUTUBE_VIDEO_DOWNLOADER_ID,
            "notebook_params": {
                "video_id": str(video["id"]),
                "video_link": str(video["link"])
            }
        }
        teste = requests.post(RUN_JOB_API, json=api_payload, headers={"Authorization":"Bearer "+DATABRICKS_AUTHORIZATION})
        print(teste.json())

def tiktok_video_downlaoder(values):
    pass
def twitter_video_downloader(values):
    pass
def linkedin_video_downlaoder(values):
    pass

# COMMAND ----------

class StartFactory:
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

def video_downloader_factory(social_network, values):
    if social_network == "youtube": youtube_video_downloader(values)
    elif social_network == "tiktok": tiktok_video_downloader(values)
    elif social_network == "twitter": twitter_video_downloader(values)
    elif social_network == "linkedin": linkedin_video_downloader(values)
    else:
        print("Rede Social não identificada.")
