# Output Schema

The actor produces raw data structured strictly for analytical extractions. To ensure compatibility with Apify's `LIMITED_PERMISSIONS` mode, all data (both videos and comments) is saved to the **Default Dataset**. 

To differentiate between a video record and a comment record, every item contains a `dataType` field.

## 1. Video Format (`dataType: "video"`)
Each video item pushed into the default dataset represents one TikTok video.

| Field             | Type   | Description                                           |
|-------------------|--------|-------------------------------------------------------|
| `dataType`        | string | Always `"video"` for these records.                   |
| `video_id`        | string | Unique identifier of the TikTok video.               |
| `video_url`       | string | Direct URL to the video.                              |
| `caption`         | string | The text caption/description of the video.            |
| `posted_at`       | string | ISO timestamp of when the video was uploaded.         |
| `author_username` | string | TikTok username of the creator.                       |
| `view_count`      | number | Total views.                                          |
| `like_count`      | number | Total likes.                                          |
| `comment_count`   | number | Total comments left on the video.                     |
| `share_count`     | number | Total shares.                                         |
| `hashtags`        | array  | Array of strings representing hashtags used.          |
| `sound_metadata`  | object | Object containing `id`, `title`, and `author`.       |
| `scrape_timestamp`| string | ISO timestamp of the exact time of extraction.        |
| `query_context`   | string | The original query that yielded this video.           |

## 2. Comment Format (`dataType: "comment"`)

If the run is configured with `fetch_comments = true`, the actor pushes comments into the **same** default dataset.

| Field               | Type   | Description                                          |
|---------------------|--------|------------------------------------------------------|
| `dataType`          | string | Always `"comment"` for these records.                |
| `video_id`          | string | ID of the video this comment belongs to.             |
| `comment_id`        | string | Unique identifier for the comment.                   |
| `comment_text`      | string | The raw text of the comment.                         |
| `comment_author`    | string | Username of the comment author.                      |
| `comment_likes`     | number | Total likes the comment received.                    |
| `comment_timestamp` | string | ISO timestamp of when the comment was posted.        |
