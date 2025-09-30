# import os
# import requests
# from bs4 import BeautifulSoup

# # Base URL for 2024 PLACE data
# BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2024/PLACE/"
# OUT_DIR = r"D:\CIR\TIGER2024_PLACE"  # change to your preferred folder

# # Create output directory if it doesn't exist
# os.makedirs(OUT_DIR, exist_ok=True)

# def list_files(base_url):
#     """Scrape directory listing for file links."""
#     response = requests.get(base_url)
#     response.raise_for_status()
#     soup = BeautifulSoup(response.text, "html.parser")
#     # Get all href links ending with .zip
#     return [link.get("href") for link in soup.find_all("a") if link.get("href", "").endswith(".zip")]

# def download_file(url, out_dir):
#     """Download a single file with streaming."""
#     local_path = os.path.join(out_dir, os.path.basename(url))
#     if os.path.exists(local_path):
#         print(f"Skipping {local_path}, already exists.")
#         return
#     print(f"Downloading {url} -> {local_path}")
#     with requests.get(url, stream=True) as r:
#         r.raise_for_status()
#         with open(local_path, "wb") as f:
#             for chunk in r.iter_content(chunk_size=8192):
#                 f.write(chunk)

# def main():
#     files = list_files(BASE_URL)
#     print(f"Found {len(files)} files to download.")
#     for f in files:
#         full_url = BASE_URL + f
#         download_file(full_url, OUT_DIR)

# if __name__ == "__main__":
#     main()

from ftplib import FTP
import os

FTP_HOST = "ftp2.census.gov"
FTP_DIR = "/geo/tiger/TIGER2024/PLACE/"
OUT_DIR = r"D:\CIR\prefect_ELT\TIGER2024_PLACE"

os.makedirs(OUT_DIR, exist_ok=True)

def download_from_ftp():
    ftp = FTP(FTP_HOST)
    ftp.login()  # anonymous login
    ftp.cwd(FTP_DIR)

    files = ftp.nlst()  # list files in directory
    print(f"Found {len(files)} files")

    for f in files:
        if not f.endswith(".zip"):
            continue
        local_path = os.path.join(OUT_DIR, f)
        if os.path.exists(local_path):
            print(f"Skipping {f}, already exists")
            continue
        print(f"Downloading {f} ...")
        with open(local_path, "wb") as fp:
            ftp.retrbinary(f"RETR {f}", fp.write)

    ftp.quit()

if __name__ == "__main__":
    download_from_ftp()
