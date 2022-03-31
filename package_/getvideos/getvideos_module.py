import requests
import numpy as np
import pandas as pd
import time

def get_no_of_pages(CHANNEL_ID, API_KEY):
    ch_url = f"https://www.googleapis.com/youtube/v3/channels?id={CHANNEL_ID}&key={API_KEY}&part=statistics"
    ch_response = requests.get(ch_url).json()
    pages_count = int(ch_response['items'][0]['statistics']['videoCount'])/50
    return np.ceil(pages_count)


def get_videos_on_four_pages(API_KEY, CHANNEL_ID, no_of_pages):
    url = f"https://www.googleapis.com/youtube/v3/search?key={API_KEY}&channelId={CHANNEL_ID}&part=snippet,id&order=date&maxResults=10000"
    count = 0
    large_list = []
    while count < no_of_pages:
        response23 = requests.get(url).json()
        large_list.append(response23['items'])
        count += 1
        try:
            if response23['nextPageToken']:
                nextpagetoken = response23['nextPageToken']
                url = f"https://www.googleapis.com/youtube/v3/search?key={API_KEY}&channelId={CHANNEL_ID}&part=snippet,id&order=date&maxResults=10000&pageToken={nextpagetoken}"
        except KeyError:
            pass
            print(f"No next page")
            break
    return large_list


def duration_in_minute(response_video_stats):
    if 'H' in response_video_stats['items'][0]['contentDetails']['duration']:
        hour = int(response_video_stats['items'][0]['contentDetails']['duration'].split('H')[0].strip('PT'))
        minute = int(response_video_stats['items'][0]['contentDetails']['duration'].split('H')[1].split('M')[0])
        seconds = int(response_video_stats['items'][0]['contentDetails']['duration'].split('H')[1].split('M')[1].strip('S'))
        hour = hour * 60
        minute = minute + hour
        if seconds > 30:
            minute += 1
        return minute
    else: 
        if 'M' in response_video_stats['items'][0]['contentDetails']['duration']:
            if 'S' in response_video_stats['items'][0]['contentDetails']['duration']:
                minute = int(response_video_stats['items'][0]['contentDetails']['duration'].split('M')[0].strip('PT'))
                seconds = int(response_video_stats['items'][0]['contentDetails']['duration'].split('M')[1].strip('S'))
                if seconds > 30:
                    minute += 1
                return minute
            else:
                minute = int(response_video_stats['items'][0]['contentDetails']['duration'].split('M')[0].strip('PT'))
                return minute
        else:
            return 1



def get_video_details(video_id, API_KEY):
    url_video_stats = "https://www.googleapis.com/youtube/v3/videos?id="+video_id+"&part=statistics,contentDetails&key="+API_KEY
    response_video_stats = requests.get(url_video_stats).json()

    view_count =  response_video_stats['items'][0]['statistics']['viewCount']
    try:
        like_count =  response_video_stats['items'][0]['statistics']['likeCount']
    except:
        pass
        like_count = 0
    try:
        dislike_count =  response_video_stats['items'][0]['statistics']['dislikeCount']
    except KeyError:
        pass
        dislike_count = 0
    try:
        comment_count =  response_video_stats['items'][0]['statistics']['commentCount']
    except KeyError:
        pass
        comment_count = 0
        
    video_duration = duration_in_minute(response_video_stats)
    return view_count, like_count,  dislike_count, comment_count, video_duration




def get_videos(API_KEY, CHANNEL_ID, no_of_pages):
    videos_pages = get_videos_on_four_pages(API_KEY, CHANNEL_ID, no_of_pages)
    
    time.sleep(1) # wait 1 second b4 for loop,so that all that from the api is collected

    video_ids = []
    video_titles = []
    upload_datetimes = []
    view_counts = []
    like_counts = []
    dislike_counts = []
    comment_counts = []
    video_durations = []
    for videos in videos_pages:
        for video in videos:
            if video['id']['kind'] == 'youtube#video':
                video_id = video['id']['videoId']
                video_title = video['snippet']['title']
                upload_datetime = video['snippet']['publishTime']

                #collecting views, like, dislike, comments
                view_count, like_count,  dislike_count, comment_count, video_duration = get_video_details(video_id, API_KEY)

                video_ids.append(video_id)
                video_titles.append(video_title)
                upload_datetimes.append(upload_datetime)
                view_counts.append(view_count)
                like_counts.append(like_count)
                dislike_counts.append(dislike_count)
                comment_counts.append(comment_count)
                video_durations.append(video_duration)

        df = pd.DataFrame({'video_id': video_ids, 'video_title': video_titles, 'upload_datetime': upload_datetimes,
                       'view_count': view_counts, 'like_count': like_counts, 'dislike_count': dislike_counts,
                       'comment_count': comment_counts, 'video_duration': video_durations})
    return df












