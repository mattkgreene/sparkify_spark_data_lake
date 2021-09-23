import zipfile

with zipfile.ZipFile("data/log-data.zip","r") as zip_ref:
    zip_ref.extractall("data/")
    

with zipfile.ZipFile("data/song-data.zip","r") as zip_ref:
    zip_ref.extractall("data/")