"""
Example script to trigger the Apify Actor via the Apify API.
Usage: 
    export APIFY_TOKEN="your_token_here"
    export ACTOR_ID="your_username/tiktok-apify-scraper"
    python run_actor.py
"""
import os
from apify_client import ApifyClient

def main():
    # Initialize the ApifyClient with your API token
    token = os.getenv("APIFY_TOKEN")
    if not token:
        print("Set APIFY_TOKEN environment variable to run this script directly.")
        print('export APIFY_TOKEN="your_token_here"')
        return
    
    client = ApifyClient(token)
    actor_id = os.getenv("ACTOR_ID", "your_username/tiktok-apify-scraper")
    
    # Define Actor input
    run_input = {
        "mode": "keyword",
        "queries": ["celsius energy", "celsius drink"],
        "max_videos": 200,
        "fetch_comments": True,
        "max_comments_per_video": 50
    }
    
    print(f"Starting run for actor: {actor_id}")
    # Start the Actor and wait for it to finish
    run = client.actor(actor_id).call(run_input=run_input)
    
    print(f"Run Finished! ID: {run['id']}")
    print(f"Default Dataset ID (Videos): {run['defaultDatasetId']}")
    
    print("Fetching actor datasets to locate 'comments-flat'...")
    # List datasets attached to this run (apify-client has methods for this, or just default to UI link)
    print("Note: Use the dataset IDs with the export_results.py script to download CSV.")

if __name__ == "__main__":
    main()
