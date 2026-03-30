#!/usr/bin/env python3
"""
YouTube Analytics — per-video data extraction → BigQuery.
Pulls analytics for every video on the channel and loads each report
into a separate BigQuery table.

Prerequisites:
  pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client google-cloud-bigquery pandas pyarrow

Setup:
  1. Go to https://console.cloud.google.com/
  2. Enable "YouTube Analytics API", "YouTube Data API v3", and "BigQuery API"
  3. Create OAuth 2.0 credentials (Desktop app) and download as client_secret.json
  4. Place client_secret.json in the same folder as this script
  5. Set GCP_PROJECT_ID and BQ_DATASET below
  6. Run the script — a browser window will open on the first run
  7. Credentials are cached in token.pickle for subsequent runs
"""

import os
import pickle

import pandas as pd
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import bigquery

# ── Configuration ─────────────────────────────────────────────────────────────

CLIENT_SECRETS_FILE = "client_secret.json"
TOKEN_FILE = "token.pickle"

SCOPES = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]

START_DATE     = "2025-01-01"
END_DATE       = "2026-01-01"
GCP_PROJECT_ID = "youtube-innconsciente"   
BQ_DATASET     = "raw_youtube_analytics"      

# ── Authentication ─────────────────────────────────────────────────────────────

def get_credentials():
    credentials = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "rb") as f:
            credentials = pickle.load(f)

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            if not os.path.exists(CLIENT_SECRETS_FILE):
                raise FileNotFoundError(
                    f"'{CLIENT_SECRETS_FILE}' not found. "
                    "Download it from Google Cloud Console (OAuth 2.0 → Desktop app)."
                )
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            credentials = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "wb") as f:
            pickle.dump(credentials, f)

    return credentials


def build_services():
    yt_credentials = get_credentials()
    analytics  = build("youtubeAnalytics", "v2", credentials=yt_credentials)
    reporting  = build("youtubereporting", "v1", credentials=yt_credentials)
    youtube    = build("youtube", "v3", credentials=yt_credentials)
    from google.oauth2 import service_account
    sa_credentials = service_account.Credentials.from_service_account_file(
        "youtube-innconsciente-5816fe914012.json",
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )
    bq_client = bigquery.Client(project=GCP_PROJECT_ID, credentials=sa_credentials)
    return analytics, reporting, youtube, bq_client

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_channel_id(youtube):
    channel_id = "UCIjLFrVmEmXhI38kTco2jyg"
    print(f"Using manual channel: {channel_id}")
    return channel_id


def get_all_video_ids(youtube, channel_id):
    """Fetch all video IDs from the channel using the uploads playlist."""
    # Get uploads playlist ID
    response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()
    uploads_playlist = (
        response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    )

    video_ids = []
    next_page_token = None

    while True:
        pl_response = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist,
            maxResults=50,
            pageToken=next_page_token,
        ).execute()

        for item in pl_response.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        next_page_token = pl_response.get("nextPageToken")
        if not next_page_token:
            break

    print(f"Found {len(video_ids)} videos on the channel.")
    return video_ids


def get_video_metadata(youtube, video_ids):
    """Fetch title, publish date, duration, and description for each video."""
    rows = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(batch),
        ).execute()
        for item in response.get("items", []):
            snippet = item["snippet"]
            stats   = item.get("statistics", {})
            rows.append({
                "video_id":        item["id"],
                "title":           snippet["title"],
                "published_at":    snippet["publishedAt"],
                "duration":        item["contentDetails"]["duration"],
                "yt_views":        stats.get("viewCount"),
                "yt_likes":        stats.get("likeCount"),
                "yt_comments":     stats.get("commentCount"),
            })
    return pd.DataFrame(rows)


def _to_dataframe(api_response):
    headers = [col["name"] for col in api_response.get("columnHeaders", [])]
    rows    = api_response.get("rows", [])
    if not rows:
        return pd.DataFrame(columns=headers)
    return pd.DataFrame(rows, columns=headers)

# ── Per-video analytics queries ───────────────────────────────────────────────

def query_video_totals(analytics, channel_id, video_ids, start_date, end_date):
    """
    Aggregate metrics for every video over the full date range.
    One row per video.
    """
    # Analytics API accepts up to ~200 video IDs in the filter at once;
    # we batch them to be safe.
    all_rows = []
    batch_size = 100

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i : i + batch_size]
        video_filter = "video==" + ",".join(batch)

        response = analytics.reports().query(
            ids="channel==UCIjLFrVmEmXhI38kTco2jyg",
            startDate=start_date,
            endDate=end_date,
            metrics=(
                "views,"
                "estimatedMinutesWatched,"
                "averageViewDuration,"
                "averageViewPercentage,"
                "subscribersGained,"
                "subscribersLost,"
                "likes,"
                "comments,"
                "shares,"
                "cardClickRate,"
                "cardTeaserClickRate,"
                "annotationClickThroughRate,"
                "annotationCloseRate"
            ),
            dimensions="video",
            filters=video_filter,
            sort="-views",
        ).execute()

        df = _to_dataframe(response)
        if not df.empty:
            all_rows.append(df)

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def query_video_daily(analytics, channel_id, video_ids, start_date, end_date):
    """
    Daily breakdown per video — useful for time-series analysis.
    """
    all_rows = []
    batch_size = 50  # smaller batches for daily granularity

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i : i + batch_size]
        video_filter = "video==" + ",".join(batch)

        response = analytics.reports().query(
            ids="channel==UCIjLFrVmEmXhI38kTco2jyg",
            startDate=start_date,
            endDate=end_date,
            metrics=(
                "views,"
                "estimatedMinutesWatched,"
                "subscribersGained,"
                "likes,"
                "comments,"
                "shares"
            ),
            dimensions="day,video",
            filters=video_filter,
            sort="day,video",
        ).execute()

        df = _to_dataframe(response)
        if not df.empty:
            all_rows.append(df)

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def query_video_traffic_sources(analytics, channel_id, video_ids, start_date, end_date):
    """Traffic source breakdown per video."""
    all_rows = []
    batch_size = 50

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i : i + batch_size]
        video_filter = "video==" + ",".join(batch)

        response = analytics.reports().query(
            ids="channel==UCIjLFrVmEmXhI38kTco2jyg",
            startDate=start_date,
            endDate=end_date,
            metrics="views,estimatedMinutesWatched",
            dimensions="video,insightTrafficSourceType",
            filters=video_filter,
            sort="video,-views",
        ).execute()

        df = _to_dataframe(response)
        if not df.empty:
            all_rows.append(df)

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def query_video_geography(analytics, channel_id, video_ids, start_date, end_date):
    """Country breakdown per video."""
    all_rows = []
    batch_size = 50

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i : i + batch_size]
        video_filter = "video==" + ",".join(batch)

        response = analytics.reports().query(
            ids="channel==UCIjLFrVmEmXhI38kTco2jyg",
            startDate=start_date,
            endDate=end_date,
            metrics="views,estimatedMinutesWatched,subscribersGained",
            dimensions="video,country",
            filters=video_filter,
            sort="video,-views",
        ).execute()

        df = _to_dataframe(response)
        if not df.empty:
            all_rows.append(df)

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def query_video_age_gender(analytics, channel_id, video_ids, start_date, end_date):
    """Age/gender breakdown per video."""
    all_rows = []

    # Age/gender only supports one video at a time via the filter
    for video_id in video_ids:
        try:
            response = analytics.reports().query(
                ids="channel==UCIjLFrVmEmXhI38kTco2jyg",
                startDate=start_date,
                endDate=end_date,
                metrics="viewerPercentage",
                dimensions="ageGroup,gender",
                filters=f"video=={video_id}",
                sort="gender,ageGroup",
            ).execute()
            df = _to_dataframe(response)
            if not df.empty:
                df.insert(0, "video", video_id)
                all_rows.append(df)
        except HttpError:
            # Some videos may have no demographic data
            pass

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def query_video_device_type(analytics, channel_id, video_ids, start_date, end_date):
    """Device type breakdown per video."""
    all_rows = []
    batch_size = 50

    for i in range(0, len(video_ids), batch_size):
        batch = video_ids[i : i + batch_size]
        video_filter = "video==" + ",".join(batch)

        response = analytics.reports().query(
            ids="channel==UCIjLFrVmEmXhI38kTco2jyg",
            startDate=start_date,
            endDate=end_date,
            metrics="views,estimatedMinutesWatched",
            dimensions="video,deviceType",
            filters=video_filter,
            sort="video,-views",
        ).execute()

        df = _to_dataframe(response)
        if not df.empty:
            all_rows.append(df)

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

# ── Enrich with titles ────────────────────────────────────────────────────────

def add_titles(df, metadata_df):
    """Add video title column after the video ID column."""
    if "video" not in df.columns or df.empty:
        return df
    id_to_title = metadata_df.set_index("video_id")["title"].to_dict()
    df = df.copy()
    df.insert(1, "title", df["video"].map(id_to_title))
    return df

# ── BigQuery upload ───────────────────────────────────────────────────────────

def upload_to_bigquery(bq_client, df, table_name):
    """Upload a DataFrame to BigQuery, replacing the table each run."""
    if df.empty:
        print(f"  {table_name}: skipped (no data)")
        return

    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # wait for completion
    print(f"  {table_name}: {len(df)} rows → {table_id}")


def ensure_dataset(bq_client):
    """Create the BigQuery dataset if it doesn't exist."""
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{BQ_DATASET}")
    dataset_ref.location = "US"
    bq_client.create_dataset(dataset_ref, exists_ok=True)

# ── Main ──────────────────────────────────────────────────────────────────────

REPORT_TYPES = [
    "channel_basic_a3",
    "channel_combined_a3",
    "channel_demographics_a1",
    "channel_device_os_a3",
    "channel_traffic_source_a3",
    "channel_reach_basic_a1",
]


def ensure_jobs(reporting):
    """Create reporting jobs for each report type if they don't exist yet."""
    existing = {
        j["reportTypeId"]: j["id"]
        for j in reporting.jobs().list().execute().get("jobs", [])
    }
    job_ids = {}
    for report_type in REPORT_TYPES:
        if report_type in existing:
            job_ids[report_type] = existing[report_type]
            print(f"  Job exists: {report_type} ({existing[report_type]})")
        else:
            job = reporting.jobs().create(body={"reportTypeId": report_type}).execute()
            job_ids[report_type] = job["id"]
            print(f"  Job created: {report_type} ({job['id']})")
    return job_ids


def download_reports(reporting, job_ids, start_date, end_date):
    """Download all available reports for each job within the date range and return DataFrames."""
    import io
    import csv
    from datetime import datetime

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

    results = {}
    for report_type, job_id in job_ids.items():
        all_rows = []
        headers  = None
        reports  = reporting.jobs().reports().list(jobId=job_id).execute()

        for report in reports.get("reports", []):
            create_time = report.get("startTime", "")
            if create_time:
                report_dt = datetime.strptime(create_time[:10], "%Y-%m-%d")
                if not (start_dt <= report_dt <= end_dt):
                    continue

            # Download the report CSV
            url  = report["downloadUrl"]
            name = url.split("v1/")[-1]
            req  = reporting.media().download(resourceName=name)
            buf  = io.BytesIO()
            from googleapiclient.http import MediaIoBaseDownload
            downloader = MediaIoBaseDownload(buf, req)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            buf.seek(0)
            reader = csv.reader(io.TextIOWrapper(buf, encoding="utf-8"))
            file_headers = next(reader, None)
            if headers is None and file_headers:
                headers = file_headers
            for row in reader:
                all_rows.append(row)

        if all_rows and headers:
            results[report_type] = pd.DataFrame(all_rows, columns=headers)
            print(f"  {report_type}: {len(all_rows)} rows")
        else:
            print(f"  {report_type}: no data in range (reports may take 24-48h to generate)")
            results[report_type] = pd.DataFrame()

    return results


def main():
    print(f"YouTube Reporting API → BigQuery | {START_DATE} → {END_DATE}\n")

    analytics, reporting, youtube, bq_client = build_services()

    print("Ensuring reporting jobs exist...")
    job_ids = ensure_jobs(reporting)

    print("\nDownloading reports...")
    reports = download_reports(reporting, job_ids, START_DATE, END_DATE)

    print("\nUploading to BigQuery...")
    ensure_dataset(bq_client)
    for table_name, df in reports.items():
        upload_to_bigquery(bq_client, df, table_name)

    print("\nDone.")


if __name__ == "__main__":
    try:
        main()
    except HttpError as e:
        print(f"HTTP error {e.resp.status}:\n{e.content.decode()}")
    except KeyboardInterrupt:
        print("Cancelled.")