# TikTok Apify Scraper

A standalone, pure data extraction module designed to run on the Apify platform.

## Overview
This actor scrapes TikTok metric and comment data based on a list of input queries (keywords, hashtags, users, or direct URLs). It avoids mixing any sort of analytics, processing, or trading logic with the raw data extraction process.

## How to Run

### Option 1: Running on the Apify Platform (UI)
1. Deploy this code as a new Apify Actor.
2. In the Actor's Input interface, fill out the required fields: `mode` (e.g., keyword) and `queries` (a list of keywords to scrape).
3. Set your desired limits (`max_videos` and `max_comments_per_video`).
4. Click **Start**.
5. Once complete, navigate to the **Storage** tab. You can download the video data directly from the default dataset.
6. To get the comments, find the dataset named `comments-flat` from your account's Storage page.

### Option 2: Running programmatically via Apify Client (Local/Remote)
Use the included `client/run_actor.py` script to trigger the task from an external service.
```bash
export APIFY_TOKEN="apify_api_token_..."
export ACTOR_ID="username/tiktok-apify-scraper"
python tiktok-apify-scraper/client/run_actor.py
```

## Exporting to CSV
Because video metrics and comments are separate entities, they are saved in separate datasets. Nested fields like `sound_metadata` can make basic CSV exports messy.

We provide a script (`client/export_results.py`) that uses `pandas` to download both datasets and flatten the structure perfectly into `.csv` format.

```bash
pip install pandas apify-client
export APIFY_TOKEN="apify_api_token_..."
export VIDEOS_DATASET_ID="dataset123..."
export COMMENTS_DATASET_ID="dataset456..."

python tiktok-apify-scraper/client/export_results.py
```
This produces `videos-flat.csv` and `comments-flat.csv`.
