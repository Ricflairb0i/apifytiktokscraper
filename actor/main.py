import asyncio
import json
import datetime
import urllib.parse
from apify import Actor
from playwright.async_api import async_playwright

def parse_video(item, query):
    try:
        video_id = item.get("id") or item.get("item_id")
        if not video_id: return None
        
        author = item.get("author", {})
        if isinstance(author, str):
            author_username = author
        else:
            author_username = author.get("uniqueId") or author.get("unique_id") or "unknown"
        
        stats = item.get("stats", {})
        if not isinstance(stats, dict): stats = {}
        
        caption = item.get("desc", "")
        
        hashtags = []
        text_extra = item.get("textExtra") or item.get("text_extra") or []
        for extra in text_extra:
            hashtag = extra.get("hashtagName")
            if hashtag:
                hashtags.append(f"#{hashtag}")
                
        sound = item.get("music", {})
        if not isinstance(sound, dict): sound = {}

        return {
            "video_id": video_id,
            "video_url": f"https://www.tiktok.com/@{author_username}/video/{video_id}",
            "caption": caption,
            "posted_at": datetime.datetime.utcfromtimestamp(int(item.get("createTime", 0))).isoformat() if item.get("createTime") else None,
            "author_username": author_username,
            "view_count": stats.get("playCount", 0),
            "like_count": stats.get("diggCount", 0),
            "comment_count": stats.get("commentCount", 0),
            "share_count": stats.get("shareCount", 0),
            "hashtags": hashtags,
            "sound_metadata": {
                "id": sound.get("id"),
                "title": sound.get("title"),
                "author": sound.get("authorName")
            },
            "scrape_timestamp": datetime.datetime.utcnow().isoformat(),
            "query_context": query
        }
    except Exception as e:
        return None

def parse_comment(c_item, video_id):
    try:
        cid = c_item.get("cid")
        if not cid: return None
        user = c_item.get("user", {})
        if not isinstance(user, dict): user = {}
        return {
            "video_id": video_id,
            "comment_id": cid,
            "comment_text": c_item.get("text", ""),
            "comment_author": user.get("unique_id") or "unknown",
            "comment_likes": c_item.get("digg_count", 0),
            "comment_timestamp": datetime.datetime.utcfromtimestamp(int(c_item.get("create_time", 0))).isoformat() if c_item.get("create_time") else None
        }
    except Exception:
        return None

async def main():
    async with Actor:
        actor_input = await Actor.get_input() or {}
        
        mode = actor_input.get('mode', 'keyword')
        queries = actor_input.get('queries', [])
        max_videos_per_query = actor_input.get('max_videos', 10)
        fetch_comments = actor_input.get('fetch_comments', False)
        max_comments = actor_input.get('max_comments_per_video', 20)
        
        Actor.log.info(f"Starting TikTok Scraper in {mode} mode.")
        Actor.log.info(f"Queries: {queries}")
        
        # Open separate dataset for comments
        comments_dataset = await Actor.open_dataset(name="comments-flat")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"]
            )
            
            for query in queries:
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800}
                )
                page = await context.new_page()
                
                collected_videos = {}
                collected_comments_count = {}
                
                async def handle_response(response):
                    if response.request.resource_type in ["xhr", "fetch"]:
                        try:
                            if "application/json" in response.headers.get("content-type", ""):
                                text = await response.text()
                                data = json.loads(text)
                                
                                # Check for videos
                                item_list = data.get("itemList") or data.get("item_list")
                                if not item_list and data.get("itemInfo"):
                                    item_list = [data.get("itemInfo").get("itemStruct")]
                                
                                if item_list and isinstance(item_list, list):
                                    for item in item_list:
                                        if isinstance(item, dict):
                                            parsed = parse_video(item, query)
                                            if parsed and parsed["video_id"] not in collected_videos:
                                                if len(collected_videos) < max_videos_per_query:
                                                    collected_videos[parsed["video_id"]] = parsed
                                                    await Actor.push_data(parsed)
                                
                                # Check for comments
                                comments_list = data.get("comments")
                                if comments_list and isinstance(comments_list, list):
                                    url = response.url
                                    parsed_url = urllib.parse.urlparse(url)
                                    aweme_id = urllib.parse.parse_qs(parsed_url.query).get("aweme_id")
                                    vid = aweme_id[0] if aweme_id else "unknown"
                                    
                                    if vid not in collected_comments_count:
                                        collected_comments_count[vid] = 0
                                        
                                    for c in comments_list:
                                        if collected_comments_count[vid] < max_comments:
                                            parsed_c = parse_comment(c, vid)
                                            if parsed_c:
                                                await comments_dataset.push_data(parsed_c)
                                                collected_comments_count[vid] += 1
                        except Exception:
                            pass
                
                page.on("response", handle_response)
                
                url = ""
                if mode == "keyword":
                    url = f"https://www.tiktok.com/search/video?q={urllib.parse.quote(query)}"
                elif mode == "hashtag":
                    url = f"https://www.tiktok.com/tag/{urllib.parse.quote(query.replace('#', ''))}"
                elif mode == "profile":
                    url = f"https://www.tiktok.com/@{urllib.parse.quote(query.replace('@', ''))}"
                elif mode == "url":
                    url = query
                    
                Actor.log.info(f"Navigating to {url}")
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                except Exception as e:
                    Actor.log.warning(f"Timeout or error navigating: {e}")
                
                # Extract Universal Data from HTML parsing initially
                try:
                    script_content = await page.evaluate("() => { const el = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__'); return el ? el.textContent : null; }")
                    if script_content:
                        data = json.loads(script_content)
                        # recursively search for "itemStruct" or "itemList"
                        def find_items(obj):
                            items = []
                            if isinstance(obj, dict):
                                if "itemStruct" in obj and isinstance(obj["itemStruct"], dict):
                                    items.append(obj["itemStruct"])
                                elif "itemList" in obj and isinstance(obj["itemList"], list):
                                    items.extend(obj["itemList"])
                                for k, v in obj.items():
                                    items.extend(find_items(v))
                            elif isinstance(obj, list):
                                for item in obj:
                                    items.extend(find_items(item))
                            return items
                        
                        html_items = find_items(data)
                        for item in html_items:
                            parsed = parse_video(item, query)
                            if parsed and parsed["video_id"] not in collected_videos:
                                if len(collected_videos) < max_videos_per_query:
                                    collected_videos[parsed["video_id"]] = parsed
                                    await Actor.push_data(parsed)
                except Exception:
                    pass
                
                # Scroll to load more videos via API
                scroll_attempts = 0
                while len(collected_videos) < max_videos_per_query and scroll_attempts < 15:
                    await page.mouse.wheel(0, 3000)
                    await page.wait_for_timeout(2000)
                    scroll_attempts += 1
                
                Actor.log.info(f"Collected {len(collected_videos)} videos for query: {query}")
                
                if fetch_comments:
                    for vid_id, vid_data in list(collected_videos.items())[:max_videos_per_query]:
                        Actor.log.info(f"Fetching comments for video: {vid_id}")
                        try:
                            video_page = await context.new_page()
                            video_page.on("response", handle_response)
                            await video_page.goto(vid_data["video_url"], wait_until="domcontentloaded", timeout=20000)
                            
                            c_scrolls = 0
                            while collected_comments_count.get(vid_id, 0) < max_comments and c_scrolls < 10:
                                await video_page.mouse.wheel(0, 2000)
                                await video_page.wait_for_timeout(2000)
                                c_scrolls += 1
                            
                            await video_page.close()
                        except Exception as e:
                            Actor.log.warning(f"Error fetching comments for {vid_id}: {e}")
                
                await context.close()
            
            await browser.close()
            
        Actor.log.info("Scraping completed.")

if __name__ == '__main__':
    asyncio.run(main())
