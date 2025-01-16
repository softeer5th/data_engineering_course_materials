import re
from typing import Literal

from googleapiclient.discovery import Resource as YoutubeResource
from googleapiclient.discovery import build


class YoutubeCommentsScraper:
    def __init__(self, api_key: str):
        """
        Initialize the YoutubeCommentsScraper object with the given API key.
        :param api_key: Youtube API key
        """
        self._youtube: YoutubeResource = build(
            "youtube", "v3", developerKey=api_key
        )

    def scrape(
        self,
        channel_ids: list[str],
        max_videos_per_channel: int = 50,
        max_comments_per_video: int = 100,
        video_sort: Literal["date", "viewCount"] = "viewCount",
        comment_sort: Literal["date", "relevance"] = "relevance",
    ) -> list[dict[str, str]]:
        """
        Scrape comments from Youtube channels.
        :param channel_ids: list of channel IDs
        :param max_videos_per_channel: maximum number of videos to scrape per channel
        :param max_comments_per_video: maximum number of comments to scrape per video
        :param video_sort: sort order for videos
        :param comment_sort: sort order for comments
        :return: list of comments
        """
        videos = self._get_videos_from_channels(
            channel_ids, max_videos_per_channel, video_sort
        )
        comments = self._get_comments_from_videos(
            videos, max_comments_per_video, comment_sort
        )
        return comments

    def _get_videos_from_channels(
        self,
        channel_ids: list[str],
        max_videos_per_channel: int,
        video_sort: Literal["date", "viewCount"],
    ) -> list[dict[str, str]]:
        """
        Get videos from a list of channels.
        :param channel_ids: list of channel IDs
        :param max_videos_per_channel: maximum number of videos to scrape per channel
        :param video_sort: sort order for videos
        :return: list of videos
        """
        videos = []
        for channel_id in channel_ids:
            videos += self._get_videos_from_channel(
                channel_id, max_videos_per_channel, video_sort
            )
        return videos

    def _get_videos_from_channel(
        self,
        channel_id: str,
        max_videos: int,
        video_sort: Literal["date", "viewCount"],
    ) -> list[dict[str, str]]:
        """
        Get videos from a channel.
        :param channel_id: channel ID
        :param max_videos: maximum number of videos to scrape
        :param video_sort: sort order for videos
        :return: list of videos
        """
        videos = []

        try:
            request = self._youtube.search().list(
                part="id,snippet",
                channelId=channel_id,
                maxResults=max_videos,
                type="video",
                order=video_sort,
            )
            response = request.execute()

            for item in response["items"]:
                videos.append(
                    {
                        "video_id": item["id"]["videoId"],
                        "title": item["snippet"]["title"],
                        "published_at": item["snippet"]["publishedAt"],
                        "channel_id": channel_id,
                        "channel_title": item["snippet"]["channelTitle"],
                    }
                )

        except Exception as e:
            print(f"Error fetching videos for channel {channel_id}: {str(e)}")
            return []

        return videos

    def _get_comments_from_videos(
        self,
        videos: list[dict[str, str]],
        max_comments_per_video: int,
        comment_sort: Literal["date", "relevance"],
    ) -> list[dict[str, str]]:
        """
        Get comments from a list of videos.
        :param videos: list of videos
        :param max_comments_per_video: maximum number of comments to scrape per video
        :param comment_sort: sort order for comments
        :return: list of comments
        """
        comments = []
        for video in videos:
            comments += self._get_comments_from_video(
                video, max_comments_per_video, comment_sort
            )
            print(f"Collected {len(comments)} comments so far...")
        return comments

    def _get_comments_from_video(
        self,
        video: dict[str, str],
        max_comments: int,
        comment_sort: Literal["date", "relevance"],
    ) -> list[dict[str, str]]:
        """
        Get comments from a video.
        :param video: video dictionary
        :param max_comments: maximum number of comments to scrape
        :param comment_sort: sort order for comments
        :return: list of comments
        """
        comments = []
        try:
            request = self._youtube.commentThreads().list(
                part="snippet",
                videoId=video["video_id"],
                maxResults=max_comments,
                textFormat="plainText",
                order=comment_sort,
            )
            response = request.execute()

            for item in response["items"]:
                comment = item["snippet"]["topLevelComment"]["snippet"]
                text = self._preprocess_text(comment["textDisplay"])

                if self._is_valid_comment(text):
                    comments.append(
                        {
                            "text": text,
                            "likes": comment["likeCount"],
                            "published_at": comment["publishedAt"],
                            "video_id": video["video_id"],
                            "vidie_title": video["title"],
                            "video_published_at": video["published_at"],
                            "channel_id": video["channel_id"],
                            "channel_title": video["channel_title"],
                        }
                    )

        except Exception as e:
            print(
                f"Error fetching comments for video {video['video_id']}: {str(e)}"
            )

        return comments

    def _preprocess_text(self, text: str) -> str:
        """
        Preprocess text.
        :param text: input text
        :return: preprocessed text
        """

        # Remove HTML tags
        text = re.sub(r"<[^>]+>", "", text)
        # Remove special characters
        text = re.sub(r"[^\w\s가-힣]", "", text)
        # Remove URLs
        text = re.sub(r"http\S+|www.\S+", "", text)
        # Remove emojis
        text = re.sub(r"\([^)]*\)", "", text)
        # Remove extra whitespaces
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _is_valid_comment(self, text: str) -> bool:
        """
        Check if a comment is valid.
        :param text: comment text
        :return: True if the comment is valid, False otherwise
        """

        if len(re.sub(r"\s", "", text)) < 10:
            return False

        spam_keywords = [
            "광고",
            "홍보",
            "섭외",
            "문의",
            "클릭",
            "링크",
            "채널",
            "주소",
        ]

        if any(keyword in text for keyword in spam_keywords):
            return False

        return True
