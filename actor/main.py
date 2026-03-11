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
                
                collected_videos = {}
                collected_comments_count = {}
                
                # DIAGNOSTICS TRACKING
                request_counts = {"document": 0, "xhr": 0, "fetch": 0, "script": 0, "websocket": 0, "other": 0}
                endpoint_family_counts = {
                    "api_search": 0, "api_recommend": 0, "api_item": 0, "api_post": 0, 
                    "graphql": 0, "discover": 0, "feed": 0, "general": 0, "other": 0
                }
                matched_request_urls = []
                matched_response_urls = []
                
                page = await context.new_page()
                
                def log_request(request):
                    rtype = request.resource_type
                    if rtype in request_counts:
                        request_counts[rtype] += 1
                    else:
                        request_counts["other"] += 1
                        
                    url = request.url.lower()
                    query_clean = urllib.parse.quote(query).lower()
                    query_space = query.replace(' ', '%20').lower()
                    has_query = query_clean in url or query_space in url
                    
                    is_api = "/api/" in url or "/graphql" in url or "/search" in url or "/discover" in url or "/recommend" in url or "/feed" in url or "/list" in url or "/query" in url or "/aweme" in url or "/item" in url or "/post" in url or "/challenge" in url or "/general" in url
                    if has_query or is_api:
                        matched_request_urls.append(url)
                        if "/api/search/" in url: endpoint_family_counts["api_search"] += 1
                        elif "/api/recommend/" in url: endpoint_family_counts["api_recommend"] += 1
                        elif "/api/item/" in url: endpoint_family_counts["api_item"] += 1
                        elif "/api/post/" in url: endpoint_family_counts["api_post"] += 1
                        elif "/graphql" in url: endpoint_family_counts["graphql"] += 1
                        elif "/discover" in url: endpoint_family_counts["discover"] += 1
                        elif "/feed" in url: endpoint_family_counts["feed"] += 1
                        elif "/general" in url: endpoint_family_counts["general"] += 1
                        else: endpoint_family_counts["other"] += 1

                async def handle_response(response):
                    if response.request.resource_type in ["xhr", "fetch"]:
                        content_type = response.headers.get("content-type", "")
                        if "application/json" in content_type:
                            url = response.url
                            url_lower = url.lower()
                            query_clean = urllib.parse.quote(query).lower()
                            query_space = query.replace(' ', '%20').lower()
                            
                            is_target = "/api/" in url_lower or "/graphql" in url_lower or "/search" in url_lower or "/discover" in url_lower or "/recommend" in url_lower or "/feed" in url_lower or "/list" in url_lower or "/query" in url_lower or "/aweme" in url_lower or "/item" in url_lower or "/post" in url_lower or "/challenge" in url_lower or "/general" in url_lower
                            
                            if is_target or query_clean in url_lower or query_space in url_lower:
                                matched_response_urls.append(url)
                                try:
                                    text = await response.text()
                                    data = json.loads(text)
                                    if isinstance(data, dict):
                                        Actor.log.info(f"[DEEP INSPECT] Target URL Found: {url}")
                                        top_keys = list(data.keys())
                                        Actor.log.info(f"   => Top-level keys: {top_keys}")
                                        
                                        candidate_arrays = []
                                        if "data" in data:
                                            dval = data["data"]
                                            if isinstance(dval, dict):
                                                Actor.log.info(f"   => Keys in 'data': {list(dval.keys())}")
                                                for k, v in dval.items():
                                                    if isinstance(v, list) and len(v) > 0:
                                                        candidate_arrays.append((f"data.{k}", v))
                                                    elif isinstance(v, dict):
                                                        for nk, nv in v.items():
                                                            if isinstance(nv, list) and len(nv) > 0:
                                                                candidate_arrays.append((f"data.{k}.{nk}", nv))
                                            elif isinstance(dval, list) and len(dval) > 0:
                                                candidate_arrays.append(("data", dval))
                                                
                                        for cname, carr in candidate_arrays:
                                            Actor.log.info(f"   => Inspecting array '{cname}' (len {len(carr)})")
                                            for idx, cobj in enumerate(carr[:2]):
                                                if isinstance(cobj, dict):
                                                    ckeys = list(cobj.keys())
                                                    expected = ["id", "item_id", "aweme_id", "desc", "title", "caption", "author", "author_info", "video", "stats", "statistics", "create_time"]
                                                    found = [f for f in expected if f in ckeys]
                                                    Actor.log.info(f"       [{idx}] fields: {found}")
                                                    
                                        # Walk arrays under data and nested objects under data
                                        video_items = []
                                        def extract_videos(obj, depth=0):
                                            if depth > 5: return
                                            if isinstance(obj, dict):
                                                keys = obj.keys()
                                                has_id = any(k in keys for k in ["id", "item_id", "aweme_id", "itemId"])
                                                has_desc = any(k in keys for k in ["desc", "title", "caption"])
                                                has_author = any(k in keys for k in ["author", "author_info", "authorInfo"])
                                                has_video = "video" in keys
                                                has_stats = any(k in keys for k in ["stats", "statistics", "statsV2"])
                                                
                                                if "item" in obj and isinstance(obj["item"], dict) and "video" in obj["item"]:
                                                    extract_videos(obj["item"], depth+1)
                                                elif has_id and (has_video or has_author or has_stats):
                                                    video_items.append(obj)
                                                else:
                                                    for v in obj.values():
                                                        if isinstance(v, (dict, list)):
                                                            extract_videos(v, depth+1)
                                            elif isinstance(obj, list):
                                                for item in obj:
                                                    if isinstance(item, (dict, list)):
                                                        extract_videos(item, depth+1)
                                                        
                                        if isinstance(data, dict):
                                            if "data" in data and isinstance(data["data"], (dict, list)):
                                                extract_videos(data["data"])
                                            elif "itemList" in data or "item_list" in data or "aweme_list" in data:
                                                lst = data.get("itemList") or data.get("item_list") or data.get("aweme_list")
                                                if isinstance(lst, list): extract_videos(lst)
                                            else:
                                                extract_videos(data)
        
                                        if video_items:
                                            for item in video_items:
                                                summary = {
                                                    "probable_id": item.get("id") or item.get("item_id") or item.get("aweme_id") or item.get("itemId") or "unknown",
                                                    "has_caption": any(k in item for k in ["desc", "title", "caption"]),
                                                    "has_author": any(k in item for k in ["author", "author_info", "authorInfo"]),
                                                    "has_video": "video" in item,
                                                    "has_stats": any(k in item for k in ["stats", "statistics", "statsV2"])
                                                }
                                                Actor.log.info(f"[NETWORK VIDEO SUMMARY] Extracted structure: {summary}")
                                                    
                                                parsed = parse_video(item, query)
                                                if parsed and parsed["video_id"] not in collected_videos:
                                                    if len(collected_videos) < max_videos_per_query:
                                                        collected_videos[parsed["video_id"]] = parsed
                                                        await Actor.push_data(parsed)
                                except Exception as e:
                                    pass

                page.on("request", log_request)
                page.on("response", handle_response)
                
                # Strategies Loop
                strategies = [
                    {"name": "DIRECT_VIDEO_SEARCH", "url": f"https://www.tiktok.com/search/video?q={urllib.parse.quote(query)}"},
                    {"name": "GENERIC_SEARCH", "url": f"https://www.tiktok.com/search?q={urllib.parse.quote(query)}"},
                    {"name": "UI_INTERACTION_ON_CURRENT_PAGE", "url": None}
                ]
                
                diagnostics_state = {}
                
                for strat_idx, strategy in enumerate(strategies):
                    if len(collected_videos) >= max_videos_per_query:
                        break
                        
                    Actor.log.info(f"--- ATTEMPTING STRATEGY {strat_idx + 1}/{len(strategies)}: {strategy['name']} ---")
                    
                    try:
                        if strategy["url"]:
                            Actor.log.info(f"Navigating to {strategy['url']}")
                            await page.goto(strategy["url"], wait_until="networkidle", timeout=45000)
                        
                        title = await page.title()
                        final_url = page.url
                        Actor.log.info(f"[STRATEGY] Page Title: {title}")
                        Actor.log.info(f"[STRATEGY] Final URL: {final_url}")
                        
                        # Apply interactions for ALL strategies (but primarily meant for strategy 3)
                        Actor.log.info("Performing user-like interactions to trigger client fetch...")
                        await page.mouse.click(100, 100)
                        await page.mouse.move(200, 300, steps=10)
                        await page.mouse.move(400, 500, steps=10)
                        # Tab navs can sometimes skip past invisible traps
                        for _ in range(3): await page.keyboard.press("Tab")
                        await page.wait_for_timeout(1000)
                        
                        # Attempt to click "Videos" tab if present
                        try:
                            videos_tab = page.locator("text='Videos'").first
                            if await videos_tab.is_visible(timeout=2000):
                                Actor.log.info("Clicking 'Videos' tab to force load...")
                                await videos_tab.click()
                                await page.wait_for_timeout(3000)
                        except Exception:
                            pass
                            
                        # Gradual scrolling
                        for _ in range(4):
                            await page.mouse.wheel(0, 500)
                            await page.wait_for_timeout(1500)
                            
                        # Run Diagnostics specific to DOM state at the end of each strategy
                        body_text = await page.evaluate("document.body.innerText || ''")
                        body_text_lower = body_text.lower()
                        words_to_check = ["video", "celsius", "energy", "for you", "related", "users", "top"]
                        found_words = [w for w in words_to_check if w in body_text_lower]
                        
                        # Count DOM video candidates
                        dom_video_links = await page.locator("a[href*='/video/']").count()
                        dom_at_links = await page.locator("a[href*='/@']").count()
                        
                        diagnostics_state = {
                            "title": title,
                            "final_url": final_url,
                            "body_length": len(body_text),
                            "keywords_found": found_words,
                            "dom_links_containing_video": dom_video_links,
                            "dom_links_containing_at": dom_at_links
                        }
                        Actor.log.info(f"[STRATEGY DOM DIAGNOSTICS] {diagnostics_state}")
                        
                        # Hard DOM Extraction Fallback Check
                        if dom_video_links > 0:
                            Actor.log.info("Found /video/ links in DOM. Attempting fallback parse.")
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
                                except Exception:
                                    pass
                                    
                        if len(collected_videos) > 0:
                            Actor.log.info(f"Strategy {strategy['name']} yielded results! Skipping remaining strategies.")
                            break
                            
                    except Exception as e:
                        Actor.log.warning(f"Strategy {strategy['name']} threw an exception: {e}")

                # HARD DIAGNOSTICS & CONCLUSION FOR QUERY
                Actor.log.info("--- END OF SCRAPING PIPELINE, EVALUATING RESULTS ---")
                
                if len(collected_videos) == 0:
                    Actor.log.error(f"FAILURE: 0 videos found for query: {query}. Saving hard diagnostic artifacts.")
                    # 1. Screenshot
                    try:
                        screenshot = await page.screenshot(full_page=True)
                        query_slug = query.replace(" ", "_").lower()
                        await Actor.set_value(f"screenshot_{query_slug}", screenshot, content_type="image/jpeg")
                        Actor.log.info(f"Saved screenshot to KeyValueStore as 'screenshot_{query_slug}'")
                    except Exception as e:
                        Actor.log.error(f"Failed to capture screenshot: {e}")
                    
                    # 2. Raw HTML
                    try:
                        content = await page.content()
                        await Actor.set_value(f"snapshot_{query_slug}", content, content_type="text/html")
                        Actor.log.info(f"Saved HTML snapshot to KeyValueStore as 'snapshot_{query_slug}'")
                    except Exception as e:
                        Actor.log.error(f"Failed to capture HTML snapshot: {e}")
                        
                    # 3. Debug JSON Artifact
                    try:
                        debug_data = {
                            "query": query,
                            "timestamp": datetime.datetime.utcnow().isoformat(),
                            "request_resource_counts": request_counts,
                            "network_endpoint_counts": endpoint_family_counts,
                            "matched_request_urls": matched_request_urls,
                            "matched_response_urls": matched_response_urls,
                            "final_dom_state": diagnostics_state
                        }
                        await Actor.set_value(f"debug_diagnostics_{query_slug}", debug_data)
                        Actor.log.info(f"Saved diagnostic JSON to KeyValueStore as 'debug_diagnostics_{query_slug}'")
                    except Exception as e:
                        Actor.log.error(f"Failed to save diagnostic JSON: {e}")

                    # Final Fallback Conclusion Rule Engine
                    Actor.log.info("\n--- DIAGNOSTIC CONCLUSION ---")
                    has_video_links = diagnostics_state.get("dom_links_containing_video", 0) > 0
                    has_search_api = endpoint_family_counts.get("api_search", 0) > 0
                    has_graphql = endpoint_family_counts.get("graphql", 0) > 0
                    looks_like_shell = 'tiktok' in diagnostics_state.get("final_url", "").lower()
                    
                    if has_video_links:
                        Actor.log.info("CONCLUSION: DOM rendered result cards dynamically, but network parser missed the payload shape.")
                    elif not looks_like_shell:
                        Actor.log.info("CONCLUSION: Probable environment/platform gating - final URL suggests a block or redirect.")
                    elif has_graphql and not has_search_api:
                        Actor.log.info("CONCLUSION: TikTok may be serving GraphQL queries for this account/geo instead of standard /api/search endpoints.")
                    elif has_search_api and not has_video_links:
                        Actor.log.info("CONCLUSION: Helper endpoints and skeleton fired, actual result payload (or DOM rendering) was suppressed, implying block/cookie requirement.")
                    else:
                        Actor.log.info("CONCLUSION: Shell loads but result payload completely absent. Probable need for authenticated session or different source strategy entirely.")
                    Actor.log.info("-----------------------------\n")
                else:
                    Actor.log.info(f"SUCCESS: Collected {len(collected_videos)} videos for query: {query}.")
                
                await context.close()
            
            await browser.close()
            
        Actor.log.info("Scraping finished.")

if __name__ == '__main__':
    asyncio.run(main())
