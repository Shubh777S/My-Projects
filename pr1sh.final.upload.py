import streamlit as st

from googleapiclient.discovery import build
from pprint import pprint
from googleapiclient.errors import HttpError
import pandas as pd
import streamlit as st

import mysql.connector as sql
import pymongo
from googleapiclient.discovery import build
from PIL import Image  
from pymongo import MongoClient      
import mysql.connector

import requests
from pymongo.server_api import ServerApi
from datetime import timedelta
from datetime import datetime
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client['first_pr_youtube']
mongo_collection = mongo_db["channel_details"]
# Connect to MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="laptop",
    database="youtube_data")
mycursor=mydb.cursor(buffered=True)
#start withh streamlit
st.sidebar.header("YOUTUBE DATA HARVESTING")
channel_id=st.sidebar.text_input('Enter the Channel ID:')
api_key="AIzaSyC-qf_tEyJxQdH1BoWzRN6YbfrmgmYXNKc"
api_source='youtube'
api_version="v3"
youtube=build("youtube","v3",developerKey=api_key)
#start scrapping data:
def get_channel_stats(channel_id):
    youtube = build(
       "youtube","v3",developerKey=api_key)
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= channel_id
    )
    response = request.execute()
    channel_details = dict(Channel_Name = response['items'][0]['snippet']['title'],Channel_Id = response['items'][0]['id'],Subscription_Count = response['items'][0]['statistics']['subscriberCount'],Video_Count = response['items'][0]['statistics']['videoCount'],Channel_Views =response['items'][0]['statistics']['viewCount'], Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    
    return channel_details


def get_playlist_id(api_key, channel_id):
    
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.channels().list(part="contentDetails", id=channel_id)
    
    response = request.execute()
    
    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    return playlist_id

playlist_id = get_playlist_id(api_key, channel_id)

def get_video_ids(playlist_id, max_results=30):
    video_ids = []

    request = youtube.playlistItems().list(part='contentDetails',playlistId=playlist_id, maxResults=min(max_results, 50))
    response = request.execute()

    for i in range(min(max_results, len(response['items']))):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    while len(video_ids) < max_results:
        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

        request = youtube.playlistItems().list(part='contentDetails', playlistId=playlist_id, maxResults=min(max_results - len(video_ids), 50), pageToken=next_page_token)
        response = request.execute()

        for i in range(min(max_results - len(video_ids), len(response['items']))):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])

    return video_ids[:max_results]
video_ids=get_video_ids(playlist_id, max_results=30)
def convert_duration(duration_string):
    
    duration_string = duration_string[2:]
    
    # Initialize a timedelta object with zero duration
    duration = timedelta()

    # Extract hours, minutes, and seconds from the duration string
    if 'H' in duration_string:
        hours, duration_string = duration_string.split('H')
        duration += timedelta(hours=int(hours))

    if 'M' in duration_string:
        minutes, duration_string = duration_string.split('M')
        duration += timedelta(minutes=int(minutes))

    if 'S' in duration_string:
        seconds, duration_string = duration_string.split('S')
        duration += timedelta(seconds=int(seconds))

    # Format duration as H:MM:SS
    duration_formatted = str(duration).split('.')[0]

    return duration_formatted

def convert_timestamp(timestamp):
    datetime_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    formatted_time = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_time
def get_video_details(youtube, video_ids):
    all_video_stats = []
    
    for i in range(0, len(video_ids), 30):
        request = youtube.videos().list(part='snippet,statistics,contentDetails', id=",".join(video_ids[i:i+30]))
        response = request.execute()
    
        for video in response['items']:
            video_stats = {
                'channel_name': video['snippet']['channelTitle'],
                'video_id': video['id'],
                'video_title': video['snippet']['title'],
                'video_description': video['snippet']['description'],
                'published_date': convert_timestamp(video['snippet']['publishedAt']),
                'view_count': video['statistics']['viewCount'],
                'like_count': video['statistics']['likeCount'],
                'comment_count': video['statistics'].get('commentCount'),
                'thumbnail': video['snippet']['thumbnails']['default']['url'],  # Adjust as needed
                'duration': convert_duration(video['contentDetails']['duration'])
            }
            
            all_video_stats.append(video_stats)                  
               
    return all_video_stats

def get_comment_threads(video_ids):
    comment_threads = []
    for video_id in video_ids:
        try:
            
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=10)
            response = request.execute()

            for item in response["items"]:
                comment_thread = {
                    "Comment_ID": item["id"],
                    "Video_ID": item["snippet"]["videoId"],
                    "Author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    "Text": item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "Published_At": convert_timestamp(item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                }
                comment_threads.append(comment_thread)
        except:
            pass

    return comment_threads

from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db= client['first_pr_youtube1']
collection= db["channel_details"]

def main(channel_id):
    c= get_channel_stats(channel_id)
    p= get_playlist_id(api_key, channel_id)
    vi= get_video_ids(playlist_id, max_results=30)
    v= get_video_details(youtube, video_ids)
    cm= get_comment_threads(video_ids)
    
      
    data=({"channel_information":c, 
                      "playlist_information":p,
                      "video_information":v,
                      "comment_information":cm})
    
    return data

if channel_id and st.sidebar.button("Scrap"):
    a=main(channel_id)
    st.write(a)
if channel_id and st.sidebar.button("Store In MongoDB"):
    a=main(channel_id)
    coll=client['first_pr_youtube1']['channel_details']
    try:
        coll.insert_one({"_id":channel_id,"channel_information":a['channel_information'],"playlist_information":a['playlist_information'],"video_information":a["video_information"],"comment_information":a["comment_information"]})
        st.sidebar.write("Stored Successfully")
    except:
        st.sidebar.write("This Channel Details already exists")
#now for sql-below:
if channel_id and st.sidebar.button("Migrate to SQL"):
    a = main(channel_id)
    try:
        mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube")
        mycursor.execute("USE youtube")
        mycursor.execute("CREATE TABLE IF NOT EXISTS channel (Channel_Name VARCHAR(255), Channel_Id VARCHAR(255) PRIMARY KEY, Subscription_Count INT, Video_Count INT, Channel_Views INT, Playlist_Id VARCHAR(255))")
        mycursor.execute("CREATE TABLE IF NOT EXISTS video (channel_name VARCHAR(255), video_id VARCHAR(255) PRIMARY KEY, video_title VARCHAR(255), video_description TEXT, published_date DATETIME, view_count INT, like_count INT, comment_count INT, thumbnail VARCHAR(255), duration TIME)")
        mycursor.execute("CREATE TABLE IF NOT EXISTS comments (Comment_ID VARCHAR(255), Video_ID VARCHAR(255), Author VARCHAR(255), Text TEXT, Published_At DATETIME, FOREIGN KEY (Video_ID) REFERENCES video(video_id))")
    except:
        mycursor.execute("USE youtube")
    try:
        ch_list = []
        for c in a.find({}, {"_id": 0, 'channel_information': 1}):
            ch_list.append(c['channel_information'])
        channel_information = pd.DataFrame(ch_list)
        mycursor.execute(f'INSERT INTO channel VALUES{tuple(channel_information.values[0])}')
        mydb.commit()

        vi_list = []
        for v in a.find({}, {"_id": 0, "video_information": 1}):
            for i in range(len(v["video_information"])):
                vi_list.append(v["video_information"][i])
        video_details = pd.DataFrame(vi_list)
        for _, row in video_details.iterrows():
            mycursor.execute(f"INSERT INTO video VALUES {tuple(row)}")
        mydb.commit()

        com_list = []
        for cm in a.find({}, {"_id": 0, "comment_information": 1}):
            for i in range(len(cm["comment_information"])):
                com_list.append(cm["comment_information"][i])
        comment_details = pd.DataFrame(com_list)
        for _, row in comment_details.iterrows():
            mycursor.execute(f"INSERT INTO comments VALUES {tuple(row)}")
        mydb.commit()

        st.sidebar.write("MIGRATION SUCCESS")

    except:
        st.sidebar.write("The Channel details already exist")

show = st.sidebar.radio('SELECT FROM BELOW', ('NONE', "SHOW TABLES", 'EXECUTE QUERIES'))

if show == 'SHOW TABLES':
    a = main(channel_id)
    mycursor.execute("USE youtube")
    mycursor.execute("SELECT * FROM channel, video, comments WHERE channel.Channel_Name=video.channel_name AND video.video_id=comments.Video_Id LIMIT 10")
    st.subheader("CHANNEL INFORMATION")
    st.write(pd.DataFrame(a['channel_information'], index=[0]))
    st.subheader("VIDEO INFORMATION")
    st.write(pd.DataFrame([a['video_information']], index=[0]))
    st.subheader('COMMENT INFORMATION')
    st.write(pd.DataFrame([a['comment_information']], index=[0]))
   

   