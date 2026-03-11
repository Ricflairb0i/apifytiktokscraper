import asyncio
import json
import datetime
import urllib.parse
from apify import Actor
from playwright.async_api import async_playwright

def parse_video(item, query):
    try:
        video_id = item.get("id") or item.get("item_id") or item.get("itemId") or item.get("aweme_id")
        if not video_id: return None
        
        author = item.get("author", {})
        if not isinstance(author, dict): author = {}
        
        author_username = author.get("uniqueId") or author.get("unique_id") or author.get("secUid") or "unknown"
        if not author_username and isinstance(author, str):
            author_username = author
            
        stats = item.get("stats", {}) or item.get("statistics", {}) or item.get("statsV2", {})
        if not isinstance(stats, dict): stats = {}
        
        caption = item.get("desc", "") or item.get("title", "") or item.get("caption", "")
        
        hashtags = []
        text_extra = item.get("textExtra") or item.get("text_extra") or item.get("challenges") or []
        if isinstance(text_extra, list):
            for extra in text_extra:
                if isinstance(extra, dict):
                    hashtag = extra.get("hashtagName") or extra.get("title")
                    if hashtag:
                        hashtags.append(f"#{hashtag}")
                
        sound = item.get("music", {}) or item.get("sound", {})
        if not isinstance(sound, dict): sound = {}
        
        create_time = item.get("createTime") or item.get("create_time") or 0
        posted_at = datetime.datetime.utcfromtimestamp(int(create_time)).isoformat() if create_time else None

        return {
            "dataType": "video",
            "video_id": video_id,
            "video_url": f"https://www.tiktok.com/@{author_username}/video/{video_id}",
            "caption": caption,
            "posted_at": posted_at,
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
        Actor.log.error(f"Error parsing video: {e}")
        return None

def parse_comment(c_item, video_id):
    try:
        cid = c_item.get("cid")
        if not cid: return None
        user = c_item.get("user", {})
        if not isinstance(user, dict): user = {}
        return {
            "dataType": "comment",
            "video_id": video_id,
            "comment_id": cid,
            "comment_text": c_item.get("text", ""),
            "comment_author": user.get("unique_id") or "unknown",
            "comment_likes": c_item.get("digg_count", 0),
            "comment_timestamp": datetime.datetime.utcfromtimestamp(int(c_item.get("create_time", 0))).isoformat() if c_item.get("create_time") else None
        }
    except Exception as e:
        Actor.log.error(f"Error parsing comment: {e}")
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
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-gpu",
                    "--no-sandbox"
                ]
            )
            
            for query in queries:
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800},
                    locale="en-US",
                    timezone_id="America/New_York"
                )
                page = await context.new_page()
                
                collected_videos = {}
                collected_comments_count = {}
                
                api_intercept_count = 0
                
                async def handle_response(response):
                    nonlocal api_intercept_count
                    if response.request.resource_type in ["xhr", "fetch"]:
                        content_type = response.headers.get("content-type", "")
                        if "application/json" in content_type:
                            url = response.url
                            
                            # Log all matching TikTok API endpoints
                            if any(endpoint in url for endpoint in ["/api/search/", "/api/post/", "/api/item/", "/api/recommend/", "/api/comment/"]):
                                api_intercept_count += 1
                                Actor.log.info(f"[NETWORK] Intercepted relevant API response: {url.split('?')[0]}")
                            
                            try:
                                text = await response.text()
                                data = json.loads(text)
                                
                                # Log structural data presence broadly
                                if isinstance(data, dict):
                                    if "itemList" in data or "item_list" in data or "data" in data or "itemInfo" in data:
                                        Actor.log.info(f"[NETWORK] Found potential video list structure. Keys: {list(data.keys())[:5]}")
                                
                                # Check for videos
                                item_list = data.get("itemList") or data.get("item_list") or data.get("data")
                                if not item_list and data.get("itemInfo"):
                                    Actor.log.info(f"[NETWORK] Found itemInfo struct.")
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
                                    Actor.log.info(f"[NETWORK] Found {len(comments_list)} comments in API response.")
                                    parsed_url = urllib.parse.urlparse(url)
                                    aweme_id = urllib.parse.parse_qs(parsed_url.query).get("aweme_id")
                                    vid = aweme_id[0] if aweme_id else "unknown"
                                    
                                    if vid not in collected_comments_count:
                                        collected_comments_count[vid] = 0
                                        
                                    for c in comments_list:
                                        if collected_comments_count[vid] < max_comments:
                                            parsed_c = parse_comment(c, vid)
                                            if parsed_c:
                                                await Actor.push_data(parsed_c)
                                                collected_comments_count[vid] += 1
                            except Exception as e:
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
                    
                Actor.log.info(f"Navigating to: {url}")
                try:
                    response = await page.goto(url, wait_until="networkidle", timeout=45000)
                    Actor.log.info(f"Navigation complete. Server responded with status: {response.status if response else 'UNKNOWN'}")
                except Exception as e:
                    Actor.log.warning(f"Timeout or error navigating to primary page. Attempting to continue anyway. Error: {e}")
                
                # Debugging 1: Page Title and Current URL
                try:
                    title = await page.title()
                    final_url = page.url
                    Actor.log.info(f"[DEBUG] Final Page Title: {title}")
                    Actor.log.info(f"[DEBUG] Final URL resolved to: {final_url}")
                    
                    if "captcha" in final_url.lower() or "verify" in title.lower() or "login" in title.lower():
                        Actor.log.warning("[WARNING] High likelihood of TikTok Challenge/Captcha verification taking place!")
                        # Wait a bit longer to see if it bypasses automatically
                        await page.wait_for_timeout(5000)
                except Exception as e:
                    Actor.log.error(f"[DEBUG] Could not fetch page title/URL: {e}")

                # Wait for initial content properly
                try:
                    Actor.log.info("Waiting for video elements or hydration to appear...")
                    await page.wait_for_timeout(4000)
                    
                    # Count DOM video candidates
                    dom_item_count = await page.locator("[data-e2e='search-card-user-link'], [data-e2e='video-author-avatar']").count()
                    Actor.log.info(f"[DEBUG] Found {dom_item_count} candidate video elements currently in the DOM visually.")
                except Exception as e:
                    Actor.log.warning(f"Error while waiting for DOM elements: {e}")

                # Extract Universal Data from HTML parsing initially
                Actor.log.info("Looking for __UNIVERSAL_DATA_FOR_REHYDRATION__ embedded script...")
                try:
                    script_content = await page.evaluate("() => { const el = document.getElementById('__UNIVERSAL_DATA_FOR_REHYDRATION__'); return el ? el.textContent : null; }")
                    if script_content:
                        Actor.log.info(f"[REHYDRATION] Found embedded hydration script ({len(script_content)} bytes). Parsing...")
                        data = json.loads(script_content)
                        
                        top_keys = list(data.keys())
                        Actor.log.info(f"[REHYDRATION] Top-level keys: {top_keys}")
                        for i, key in enumerate(top_keys[:3]):
                            if isinstance(data[key], dict):
                                Actor.log.info(f"[REHYDRATION] Nested keys for '{key}': {list(data[key].keys())[:10]}")
                                
                        # recursively search for anything that looks like a video object or list
                        def find_items(obj, depth=0):
                            items = []
                            if depth > 10: return items
                            if isinstance(obj, dict):
                                # Heuristics for TikTok video object
                                if ("id" in obj and "desc" in obj and "author" in obj) or \
                                   ("item_id" in obj and "video" in obj) or \
                                   ("aweme_id" in obj):
                                   items.append(obj)
                                elif str(obj.get("id", "")).isdigit() and len(str(obj.get("id", ""))) >= 18:
                                    items.append(obj)
                                else:
                                    for k, v in obj.items():
                                        items.extend(find_items(v, depth+1))
                            elif isinstance(obj, list):
                                for item in obj:
                                    items.extend(find_items(item, depth+1))
                            return items
                        
                        html_items = find_items(data)
                        
                        # Deduplicate parsed hydration items
                        deduped_html_items = {str(item.get("id", item.get("item_id", ""))): item for item in html_items if str(item.get("id", item.get("item_id", "")))}.values()
                        
                        Actor.log.info(f"[REHYDRATION] Parsed {len(deduped_html_items)} unique candidate items from static HTML state.")
                        for item in deduped_html_items:
                            parsed = parse_video(item, query)
                            if parsed and parsed["video_id"] not in collected_videos:
                                if len(collected_videos) < max_videos_per_query:
                                    collected_videos[parsed["video_id"]] = parsed
                                    await Actor.push_data(parsed)
                    else:
                        Actor.log.warning("[REHYDRATION] No hydration script found on page. DOM might be fully blockaded or requires interaction.")
                except Exception as e:
                    Actor.log.error(f"[REHYDRATION ERROR] Failed to parse HTML state: {e}")
                
                # Scroll to load more videos via API
                Actor.log.info("Beginning auto-scroll procedure to trigger lazy loading APIs...")
                scroll_attempts = 0
                while len(collected_videos) < max_videos_per_query and scroll_attempts < 10:
                    try:
                        # Ensure we scroll effectively to bottom
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await page.wait_for_timeout(3000)
                        
                        # Fallback DOM extraction strategy right off the live page inside the loop
                        vid_data_list = await page.evaluate('''() => {
                            const links = Array.from(document.querySelectorAll('a[href*="/video/"]'));
                            return links.map(a => {
                                const container = a.closest('[class*="item-container"]') || a.closest('div');
                                return {
                                    href: a.href,
                                    text: a.innerText || (container ? container.innerText : "")
                                };
                            });
                        }''')
                        
                        new_links = 0
                        for vdata in vid_data_list:
                            link = vdata.get('href', '')
                            text = vdata.get('text', '').replace('\\n', ' ').strip()
                            try:
                                vid = link.split('/video/')[1].split('?')[0]
                                if vid and vid not in collected_videos and len(collected_videos) < max_videos_per_query:
                                    username_part = link.split('/@')[1].split('/video')[0] if '/@' in link else "unknown"
                                    parsed = {
                                        "dataType": "video",
                                        "video_id": vid,
                                        "video_url": link,
                                        "caption": text if text else "Fallback Extraction - Network Blocked",
                                        "posted_at": datetime.datetime.utcnow().isoformat(),
                                        "author_username": username_part,
                                        "view_count": 0, "like_count": 0, "comment_count": 0, "share_count": 0,
                                        "hashtags": [], "sound_metadata": {},
                                        "scrape_timestamp": datetime.datetime.utcnow().isoformat(),
                                        "query_context": query
                                    }
                                    collected_videos[vid] = parsed
                                    await Actor.push_data(parsed)
                                    new_links += 1
                            except Exception:
                                pass
                                
                        if new_links > 0:
                            Actor.log.info(f"[FALLBACK DOM] Extracted {new_links} new video bounds directly from DOM. Total candidate links seen: {len(vid_data_list)}")
                        elif len(vid_data_list) > 0:
                            Actor.log.info(f"[FALLBACK DOM] Found {len(vid_data_list)} /video/ links but extracted 0 new items.")
                            
                    except Exception as e:
                        Actor.log.warning(f"Error during scroll: {e}")
                    scroll_attempts += 1
                
                Actor.log.info(f"[NETWORK TOTAL] Caught {api_intercept_count} relevant API requests during runtime.")
                Actor.log.info(f"Finished processing. Collected {len(collected_videos)} total videos for query: {query}")
                
                if fetch_comments:
                    for vid_id, vid_data in list(collected_videos.items())[:max_videos_per_query]:
                        Actor.log.info(f"Fetching comments for video: {vid_id}")
                        try:
                            video_page = await context.new_page()
                            video_page.on("response", handle_response)
                            await video_page.goto(vid_data["video_url"], wait_until="networkidle", timeout=30000)
                            
                            c_scrolls = 0
                            while collected_comments_count.get(vid_id, 0) < max_comments and c_scrolls < 10:
                                await video_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                await video_page.wait_for_timeout(2500)
                                c_scrolls += 1
                            
                            await video_page.close()
                        except Exception as e:
                            Actor.log.warning(f"Error fetching comments for {vid_id}: {e}")
                
                await context.close()
            
            await browser.close()
            
        Actor.log.info("Scraping completed.")

if __name__ == '__main__':
    asyncio.run(main())
