# Databricks notebook source
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
