"""
Standalone Apify Actor for scraping TikTok video metadata and comments.
Expected Input: mode, queries, max_videos, fetch_comments, max_comments_per_video
Outputs:
- Default Dataset (or 'videos_raw'): TikTok Video Metadata
- 'comments_flat' Dataset: Flat comment properties
"""
import asyncio
import os
import json
import datetime
from apify import Actor

def extract_videos_mock(query, max_videos):
    # Dummy implementation representing scraped video output
    videos = []
    count = min(2, max_videos)
    for i in range(count):
        videos.append({
            "video_id": f"vid_mock_{i}",
            "video_url": f"https://www.tiktok.com/@mockuser/video/vid_mock_{i}",
            "caption": f"Sample post for {query}",
            "posted_at": datetime.datetime.utcnow().isoformat(),
            "author_username": "mockuser",
            "view_count": 5000 + i,
            "like_count": 100 + i,
            "comment_count": 10 + i,
            "share_count": 5,
            "hashtags": ["#mock", f"#{query.replace(' ', '')}"],
            "sound_metadata": {"id": "123", "title": "Original Sound", "author": "mockuser"},
            "scrape_timestamp": datetime.datetime.utcnow().isoformat(),
            "query_context": query
        })
    return videos

def extract_comments_mock(video_id, max_comments):
    comments = []
    count = min(5, max_comments)
    for i in range(count):
        comments.append({
            "video_id": video_id,
            "comment_id": f"cmd_mock_{i}",
            "comment_text": "Great video!",
            "comment_author": "fan_user",
            "comment_likes": 2,
            "comment_timestamp": datetime.datetime.utcnow().isoformat()
        })
    return comments

async def main():
    async with Actor:
        # 1. Parse Input
        actor_input = await Actor.get_input() or {}
        
        mode = actor_input.get('mode', 'keyword')
        queries = actor_input.get('queries', [])
        max_videos = actor_input.get('max_videos', 100)
        fetch_comments = actor_input.get('fetch_comments', False)
        max_comments_per_video = actor_input.get('max_comments_per_video', 50)
        
        Actor.log.info(f"Starting TikTok Scraper in {mode} mode.")
        Actor.log.info(f"Queries: {queries}")
        
        # Open separate dataset for comments
        comments_dataset = await Actor.open_dataset(name="comments_flat")
        
        # 2. Scrape Videos
        # (Placeholder for real TikTok fetching logic - e.g., using Playwright or httpx)
        for query in queries:
            Actor.log.info(f"Processing query: {query}")
            # Mock Video extraction
            videos = extract_videos_mock(query, max_videos)
            
            for video in videos:
                # Store video
                await Actor.push_data(video)
                
                # 3. Scrape Comments (if requested)
                if fetch_comments:
                    Actor.log.info(f"Fetching comments for video: {video['video_id']}")
                    comments = extract_comments_mock(video['video_id'], max_comments_per_video)
                    for comment in comments:
                        await comments_dataset.push_data(comment)
                        
        Actor.log.info("Scraping completed.")

if __name__ == '__main__':
    asyncio.run(main())
