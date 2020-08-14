import requests
import json
import re
import pandas as pd
import time
import argparse
import math
from tqdm import tqdm
import datetime

from google_drive_utils import upload_df_to_gd

parser = argparse.ArgumentParser()
parser.add_argument('--github_username', required=True, type=str, help='Username for GitHub')
parser.add_argument('--access_token', required=True, type=str, help='Personal Access Token')
args = parser.parse_args()

def get_json_data_from_url(url):
    try:
        r = requests.get(url, auth=(args.github_username, args.access_token))
    except Exception as err:
        connection_error_timeout_seconds = 300
        print(f"Timing out for {connection_error_timeout_seconds}s because error thrown when requesting data from <<< {url} >>>\n{err}\n\n")
        time.sleep(connection_error_timeout_seconds)
        return None

    # Sleep and return None if URL is not working. Sleep in case non-200 is due to rate limiting.
    if r.status_code != 200:
        # Time out more if we get a 403 (telling us we are making too many calls)
        timeout_time_seconds = 10 if r.status_code == 403 or r.status_code == 433 else 0.1
        print(f"Timing out for {timeout_time_seconds} seconds after getting a {r.status_code} status code from {url}")
        time.sleep(timeout_time_seconds)
        return None

    data = json.loads(r.content)
    return data

def get_earliest_dup_date():
    # Get all issues, sorted by date, ascending
    earliest_duplicates = get_json_data_from_url("https://api.github.com/search/issues?q=label:duplicate&per_page=100&page=10&sort=created&order=asc")

    if earliest_duplicates is None:
        timeout_time_seconds = 120
        print(f"Retrying call to get earliest date in {timeout_time_seconds} seconds")
        time.sleep(timeout_time_seconds)
        return get_earliest_dup_date()

    # Take the bottom result in the list (I.e. the 1000th earliest over all issues) and get its creation date
    # We do not take the very earliest, as the earliest was from 2000, and the 100th earliest is from 2003. The 1000th is from 2008, and the next ~270,000 are from the following 12 years.
    # There is a lot of sparsity between days, meaning that we start to hit a rate limit quickly if we do not take a slightly later date.
    # We therefore do not mind sacraficing the first 1000 entries so that we do not have to cycle through 8 years of dates with queries
    earliest_date_duplicate_string = earliest_duplicates["items"][99]["created_at"]

    # Only get the date of the creation date (I.e. not time)
    earliest_date_duplicate_string = earliest_date_duplicate_string.split("T")[0]

    # Convert to datetime
    earliest_date_duplicate = datetime.datetime.strptime(earliest_date_duplicate_string, "%Y-%m-%d")

    return earliest_date_duplicate

def get_date_iteration_max():
    # Get earliest date of duplicate issue
    earliest_date = get_earliest_dup_date()

    # Find the time between now and the earliest date
    date_delta = datetime.datetime.now() - earliest_date

    # Get the number of days from this difference in time
    return date_delta.days

def iterate_date(date):
    return date + datetime.timedelta(days=1)

search_date = get_earliest_dup_date()

daily_iteration_bar = tqdm(range(get_date_iteration_max()))

for _ in daily_iteration_bar:
    search_date_string = search_date.strftime("%Y-%m-%d")

    daily_iteration_bar.set_description(f"Searching for issues on date {search_date_string}")

    issues = get_json_data_from_url(f"https://api.github.com/search/issues?q=label:duplicate+created:{search_date_string}&per_page=100&page=1&sort=created&order=asc")

    search_date = iterate_date(search_date)

    if issues is None:
        continue

    number_pages = math.ceil(issues["total_count"] / 100)

    # GitHub API Only shows the first 1000 results, meaning that we cannot get any issue data past page 10
    number_pages = min([10, number_pages])

    page_bar = tqdm(range(1, number_pages+1), position=1, leave=True)

    issue_data_list = []

    for page in page_bar:
        page_bar.set_description(f"Page number {page}")

        # Get duplicate issues
        issues = get_json_data_from_url(f"https://api.github.com/search/issues?q=label:duplicate+created:{search_date_string}&per_page=100&page={page}&sort=created&order=asc")

        # Finds all mentions of a hash followed by numbers (E.g. #1234)
        issue_finder_regex = re.compile("#\d+")

        # Removes all code between code blocks (In order to reduce size of comments and only retain more human readable bits)
        code_cleaner_regex = re.compile("```([\S\s]+)```")

        if issues is None:
            continue

        issue_bar = tqdm(issues["items"], position=2, leave=True)

        for issue in issue_bar:
            try:
                url = issue["url"]
                issue_bar.set_description(f"Scraping issue {url}")

                issue_title = issue["title"]
                issue_body_raw = issue["body"]
                issue_body = code_cleaner_regex.sub("[CODE]", issue_body_raw) if issue_body_raw is not None else issue_body_raw
                issue_labels = [x["name"] for x in issue["labels"]]
                issue_number = url.split("/")[-1]

                # Get comments
                comment_data = get_json_data_from_url(issue["comments_url"])

                if comment_data is None:
                    continue

                dup_issues = issue_finder_regex.findall("".join([x["body"] for x in comment_data]))

                # Make sure that we don't simply capture a reference to the current issue or 0
                dup_issues = [x for x in dup_issues if x != f"#{issue_number}" and x != "#0"]

                if len(dup_issues) <= 0:
                    continue

                first_dup_issue = dup_issues[0]
                duplicate_issue_url = "/".join(url.split("/")[:-1]) + dup_issues[0].replace("#", "/")

                duplicate_data = get_json_data_from_url(duplicate_issue_url)

                if duplicate_data is None:
                    continue

                duplicate_body_raw = duplicate_data["body"]
                duplicate_body = code_cleaner_regex.sub("[CODE]", duplicate_body_raw) if duplicate_body_raw is not None else duplicate_body_raw
                duplicate_title = duplicate_data["title"]
                duplicate_labels = [x["name"] for x in duplicate_data["labels"]]

                issue_data_list.append({
                    "url": url,
                    "issue_title": issue_title,
                    "issue_body": issue_body,
                    "issue_body_raw": issue_body_raw,
                    "issue_labels": issue_labels,
                    "dup_issues": dup_issues,
                    "first_dup_issue_url": duplicate_issue_url,
                    "duplicate_body": duplicate_body,
                    "duplicate_body_raw": duplicate_body_raw,
                    "duplicate_title": duplicate_title,
                    "duplicate_labels": duplicate_labels
                })
            except Exception as e:
                current_url = issue["url"]
                print(f"Error when processing/scraping {current_url}:\n{e}\n\n")

    if len(issue_data_list) > 0:
        file_date_string = search_date_string.replace("-", "_")
        upload_df_to_gd(f"github_issues_{file_date_string}.csv", pd.DataFrame(issue_data_list), "1lbS874mV9ImWe8PDZNucOds8hX0yjFWe")
