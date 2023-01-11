# Databricks notebook source
def youtube_video_downloader(values):
    for video in values:
        print(video["title"])
        api_payload = {
            "job_id": YOUTUBE_VIDEO_DOWNLOADER_ID,
            "notebook_params": {
                "video_title": str(video["title"]),
                "video_id": str(video["id"]),
                "video_link": str(video["link"])
            }
        }
        teste = requests.post(RUN_JOB_API, json=api_payload, headers={"Authorization":"Bearer dapi37471f3e8af728275efc027d77f9535a-3"})
        print(teste.json())

def tiktok_video_downlaoder(values):
    pass
def twitter_video_downloader(values):
    pass
def linkedin_video_downlaoder(values):
    pass

# COMMAND ----------

def video_downloader_factory(social_network, values):
    if social_network == "youtube": youtube_video_downloader(values)
    elif social_network == "tiktok": tiktok_video_downloader(values)
    elif social_network == "twitter": twitter_video_downloader(values)
    elif social_network == "linkedin": linkedin_video_downloader(values)
    else:
        print("Rede Social não identificada.")
