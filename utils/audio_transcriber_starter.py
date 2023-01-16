# Databricks notebook source
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
