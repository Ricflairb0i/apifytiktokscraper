"""
Script to download dataset items from Apify and export them to flat CSV.
Requires python-dotenv, apify-client, and pandas.
"""
import os
import pandas as pd
from apify_client import ApifyClient

def export_dataset_to_csv(client: ApifyClient, dataset_id: str, output_csv_path: str, is_video: bool = True):
    print(f"Fetching data from dataset: {dataset_id}")
    dataset_client = client.dataset(dataset_id)
    items = dataset_client.list_items().items
    
    if not items:
        print(f"Dataset {dataset_id} is empty.")
        return
        
    df = pd.DataFrame(items)
    
    # Flattening specific fields like sound_metadata if video
    if is_video and 'sound_metadata' in df.columns:
        # Extract fields from nested dictionary if they exist
        df['sound_id'] = df['sound_metadata'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)
        df['sound_title'] = df['sound_metadata'].apply(lambda x: x.get('title') if isinstance(x, dict) else None)
        df['sound_author'] = df['sound_metadata'].apply(lambda x: x.get('author') if isinstance(x, dict) else None)
        # Drop the original nested column to keep CSV completely flat
        df.drop(columns=['sound_metadata'], inplace=True, errors='ignore')
    
    # Save to CSV
    df.to_csv(output_csv_path, index=False)
    print(f"Exported {len(items)} items to {output_csv_path}")

def main():
    token = os.getenv("APIFY_TOKEN")
    if not token:
        print("Please set the APIFY_TOKEN environment variable.")
        return
        
    client = ApifyClient(token)
    
    # You would typically pass these as arguments
    videos_dataset_id = os.getenv("VIDEOS_DATASET_ID")
    comments_dataset_id = os.getenv("COMMENTS_DATASET_ID")
    
    if videos_dataset_id:
        export_dataset_to_csv(client, videos_dataset_id, "videos-flat.csv", is_video=True)
    else:
        print("VIDEOS_DATASET_ID not set, skipping videos export.")
        
    if comments_dataset_id:
        export_dataset_to_csv(client, comments_dataset_id, "comments-flat.csv", is_video=False)
    else:
        print("COMMENTS_DATASET_ID not set, skipping comments export.")

if __name__ == "__main__":
    main()
