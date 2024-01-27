import requests
import re
import subprocess
from multiprocessing import Pool
import time
import random
import os


def get_thread_urls(url):
    response = requests.get(url)

    if response.status_code == 200:
        xml_content = response.text

        regex_pattern = r"https://raypeatforum\.com/community/threads/[\w-]+\.[\d]+"
        found_urls = re.findall(regex_pattern, xml_content)

        return found_urls
    else:
        print(f"Failed to retrieve XML content. Status code: {response.status_code}")


def store_all_thread_urls(threads_path):
    sitemap1 = "https://raypeatforum.com/community/sitemap-1.xml"
    sitemap2 = "https://raypeatforum.com/community/sitemap-2.xml"

    one = get_thread_urls(sitemap1)
    two = get_thread_urls(sitemap2)

    all_urls = one + two

    with open(threads_path, "w") as f:
        for url in all_urls:
            f.write(f"{url}\n")

    return all_urls


def execute_wget(url, backup_path):
    thread_page_regex = f"^{re.escape(url)}(/page-[0-9]+)?$"
    data_domain_regex = "data\\.raypeatforum\\.com/.*"
    reject_data_domain_regex = "data\\.raypeatforum\\.com/avatars/.*"
    attachments_domain_regex = "raypeatforum\\.com/community/attachments/.*"
    accept_regex = (
        f"({thread_page_regex})|({data_domain_regex})|({attachments_domain_regex})"
    )

    command = f"""
    wget --recursive --no-clobber --page-requisites --content-disposition --adjust-extension --span-hosts \
    --convert-links --restrict-file-names=windows \
    --domains raypeatforum.com,data.raypeatforum.com --no-parent \
    --accept-regex '{accept_regex}' \
    --reject-regex '{reject_data_domain_regex}' \
    --referer='{url}' \
    --execute robots=off \
    --user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3" \
    '{url}'
    """

    subprocess.run(command, shell=True, check=True, cwd=backup_path)


def thread_download(url, backup_path, completed_path, max_wait_seconds=5):
    time.sleep(random.uniform(0, max_wait_seconds))

    execute_wget(url, backup_path)

    with open(completed_path, "a") as f:
        f.write(url + "\n")


if __name__ == "__main__":
    MAX_THREADS = 5
    current_directory = os.getcwd()
    threads_path = os.path.join(current_directory, "data", "threads.txt")
    completed_path = os.path.join(current_directory, "data", "completed_urls.txt")
    backup_path = os.path.join(current_directory, "backup")

    try:
        with open(threads_path, "r") as file:
            all_urls = {line.strip() for line in file}
    except FileNotFoundError:
        all_urls = set(store_all_thread_urls(threads_path))

    try:
        with open(completed_path, "r") as comp_file:
            completed_urls = {line.strip() for line in comp_file}
    except FileNotFoundError:
        completed_urls = set()

    urls_to_process = list(all_urls - completed_urls)

    print(f"finished: {len(completed_urls)}; remaining: {len(urls_to_process)}")

    with Pool(MAX_THREADS) as p:
        p.starmap(
            thread_download,
            [(url, backup_path, completed_path) for url in urls_to_process],
        )
