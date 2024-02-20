from googleapiclient.discovery import build
from pprint import pprint
from googleapiclient.errors import HttpError
import pandas as pd
import mysql.connector as sql
import mysql.connector
import pymongo
from googleapiclient.discovery import build
from PIL import Image
import requests
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import timedelta
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.mysql import insert
import streamlit as st
api_service_name = "youtube"
api_version = "v3"
api_key = 'enter your api key'
youtube = build(api_service_name, api_version,developerKey=api_key)

#streamlit
# Logo
image_path = "./YT_logo.png"
st.image(image_path, width=100)

# Adding a title to your Streamlit app with custom styling
st.markdown(
    '<h1 style="text-align:center; color:teal; font-size:2em; margin-top: -65px;">YOUTUBE DATA HARVESTING AND WAREHOUSING</h1>',
    unsafe_allow_html=True)

#start scrapping data:

def get_channel_stats(channel_id):
    youtube = build(
       "youtube","v3",developerKey=api_key)
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id= channel_id
    )
    response = request.execute()
    Channel_Data = dict(Channel_Name = response['items'][0]['snippet']['title'],Channel_Id = response['items'][0]['id'],Subscription_Count = response['items'][0]['statistics']['subscriberCount'],Video_Count = response['items'][0]['statistics']['videoCount'],Channel_Views =response['items'][0]['statistics']['viewCount'], Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    
    return Channel_Data

def get_playlist_id(api_key, channel_id):
    
    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.channels().list(part="contentDetails", id=channel_id)
    
    response = request.execute()
    
    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    return playlist_id

def get_playlist_details(channel_id):
    playlist_details=[]
    request=youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=100)
    response=request.execute()
    for i in response['items']:
        details=dict(playlist_id=i['id'],
                     playlist_title=i['snippet']['title'],
                     published_date=convert_timestamp(i['snippet']['publishedAt']),
                     total_videos=i['contentDetails']['itemCount'],
                     channel_id=channel_id
                    )
        playlist_details.append(details)
    return playlist_details

def get_video_ids(youtube, playlist_id, max_results=30):
    video_ids = []

    request = youtube.playlistItems().list(part='contentDetails', playlistId=playlist_id, maxResults=min(max_results, 50))
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

def convert_duration(duration_string):
    # Remove "PT" prefix
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

#mongo db:
from pymongo import MongoClient
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:____/")
db= client['first_pr_youtube']
coll1= db["channel_details"]

def main(channel_id):
    
    channel_data = get_channel_stats(channel_id)
    playlist_id= get_playlist_id(api_key, channel_id)
    playlist_details=get_playlist_details(channel_id)
    video_ids = get_video_ids(youtube, playlist_id)
    video_details= get_video_details(youtube, video_ids)
    comments_details= get_comment_threads(video_ids)
    
      
    data=({"channel_information":channel_data, 
                      "playlist_information":playlist_details,
                      "video_information":video_details,
                      "comment_information":comments_details})
    
    return data

#now connect all functions to streamlit
channel_id=st.sidebar.text_input('Enter the Channel ID:')
if channel_id and st.sidebar.button("Scrap"):
    a=main(channel_id)
    st.write(a)
if channel_id and st.sidebar.button("Store in MongoDB"):
    a=main(channel_id)
    coll1=client['first_pr_youtube']['channel_details']
    try:
        coll1.insert_one({"_id":channel_id,"channel_information":a['channel_information'],"playlist_information":a['playlist_information'],"video_information":a["video_information"],"comment_information":a["comment_information"]})
        st.sidebar.write("Stored Successfully")
    except:
        st.sidebar.write("This Channel Details already exists")

#migrate to MY SQL:
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="abcd")
cursor = mydb.cursor()

def channels_table():
    
    # Connect to MySQL
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abcd",
        database="youtube_data")
        
    cursor = mydb.cursor()

    drop_query = "drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(Channel_Name varchar(100),
                        Channel_Id varchar(80) primary key, 
                        Subscription_Count bigint, 
                        Video_Count int,
                        Channel_Views bigint,
                        Playlist_Id varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Channels Table alredy created")    


    # Retrieve data from MongoDB collection
    ch_list=[]
    db= client['first_pr_youtube']
    coll1 = db["channel_details"]

    for channel_data in coll1.find({}, {"_id": 0, 'channel_information': 1}):
        ch_list.append(channel_data['channel_information'])
        c_details=pd.DataFrame(ch_list)
    
    for index,row in c_details.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscription_Count,
                                                    Video_Count,
                                                    Channel_Views,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''
            

        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Video_Count'],
                row['Channel_Views'],
                row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            st.write("Channels values are already inserted")

def playlists_table():
    
    # Connect to MySQL
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abcd",
        database="youtube_data")
        
    cursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(playlist_id varchar(100),
                        playlist_title varchar(80),
                        published_date timestamp,
                        total_videos int,
                        channel_id varchar(100))'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Playlists Table alredy created")    

    # Retrieve data from MongoDB collection
    pl_list = []
    db= client['first_pr_youtube']
    coll1 = db["channel_details"]

    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
            pl_details= pd.DataFrame(pl_list)
    
    for index,row in pl_details.iterrows():
        insert_query = '''INSERT into playlists(playlist_id,
                                                    playlist_title,
                                                    published_date,
                                                    total_videos,
                                                    channel_id)
                                        VALUES(%s,%s,%s,%s,%s)'''            
        values =(
                row['playlist_id'],
                row['playlist_title'],
                row['published_date'],
                row['total_videos'],
                row['channel_id'])
                
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            st.write("Playlists values are already inserted")

def videos_table():

    # Connect to MySQL
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abcd",
        database="youtube_data")
        
    cursor = mydb.cursor()

    drop_query = "drop table if exists videos"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists videos(
                        channel_name VARCHAR(50),
                        video_id VARCHAR(50),
                        video_title VARCHAR(255),
                        video_description TEXT,
                        published_date DATETIME,
                        view_count INT,
                        like_count INT,
                        comment_count INT,
                        thumbnail VARCHAR(255),
                        duration TIME
                        )''' 
                        
        cursor.execute(create_query)             
        mydb.commit()
    except:
        st.write("Videos Table alrady created")
        
    # Retrieve data from MongoDB collection
    vi_list = []
    db= client['first_pr_youtube']
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
            vi_details = pd.DataFrame(vi_list)
    
    for index, row in vi_details.iterrows():
        insert_query = '''
                    INSERT INTO videos (channel_name,
                        video_id,
                        video_title, 
                        video_description,
                        published_date,
                        view_count, 
                        like_count,
                        comment_count, 
                        thumbnail, 
                        duration
                        )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

                '''
        values = (
                    row['channel_name'],
                    row['video_id'],
                    row['video_title'],
                    row['video_description'],
                    row['published_date'],
                    row['view_count'],
                    row['like_count'],
                    row['comment_count'],
                    row['thumbnail'],
                    row['duration'])
                                
        try:    
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("videos values already inserted in the table")

def comments_table():
    
    # Connect to MySQL
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="abcd",
        database="youtube_data")
        
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(
                        Comment_ID VARCHAR(50) ,
                        Video_ID VARCHAR(50),
                        Author VARCHAR(50),
                        Text TEXT,
                        Published_At DATETIME)'''
        
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        st.write("Comments Table already created")
        
    # Retrieve data from MongoDB collection
    com_list = []

    db= client['first_pr_youtube']
    coll1 = db["channel_details"]

    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
            cm_details= pd.DataFrame(com_list)


    for index, row in cm_details.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_ID,
                                      Video_ID ,
                                      Author,
                                      Text,
                                      Published_At)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['Comment_ID'],
                row['Video_ID'],
                row['Author'],
                row['Text'],
                row['Published_At']
            )
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
               st.write("This comments are already exist in comments table")

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"

def show_channels_table():
    
    db= client['first_pr_youtube']
    coll1 = db["channel_details"]
    ch_list=[]
    for channel_data in coll1.find({}, {"_id": 0, 'channel_information': 1}):
        ch_list.append(channel_data['channel_information'])
    channels_table=st.dataframe(ch_list)
    return channels_table               

def show_playlists_table():
  
    db = client["first_pr_youtube"]
    coll1 =db["channel_details"]
    
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    playlists_table= st.dataframe(pl_list)
    return playlists_table

def show_videos_table():
    vi_list = []
    db= client['first_pr_youtube']
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table


def show_comments_table():
    com_list = []

    db= client['first_pr_youtube']
    coll1 = db["channel_details"]

    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table

  #execute streamlit to migrate in sql.

if channel_id:
    migrate_button_clicked = st.sidebar.button("Migrate to SQL")

    if migrate_button_clicked:
        try: 
            display = tables()
            st.success(display)
        except:
            st.sidebar.write("The Channel details already exist")

    # Now, you can display other options only if the "Migrate to SQL" button is clicked
    show = st.sidebar.radio('Select from below', ('None', 'Show Table', 'FAQ'))

    FAQ = None 

    if show == "Show Table":
        show_table = st.radio("Select Table", (":red[Channels]", ":orange[Playlists]", ":blue[Videos]", ":green[Comments]"))

        if show_table == ":red[Channels]":
            show_channels_table()
        elif show_table == ":orange[Playlists]":
            show_playlists_table()
        elif show_table == ":blue[Videos]":
            show_videos_table()
        elif show_table == ":green[Comments]":
            show_comments_table()

    if show=='FAQ':
    
        FAQ = st.selectbox(
        'Select Your Question',
        ('1. What are the names of all the videos and their corresponding channels?',
        '2. Which channels have the most number of videos, and how many videos do they have?',
        '3. What are the top 10 most viewed videos and their respective channels?',
        '4. How many comments were made on each video, and what are their corresponding video names?',
        '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6. What is the total number of likes for each video, and what are their corresponding video names?',
        '7. What is the total number of views for each channel, and what are their corresponding channel names?',
        '8. What are the names of all the channels that have published videos in the year 2022?',
        '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

     
    if FAQ == '1. What are the names of all the videos and their corresponding channels?':
        query1 = "SELECT video_title AS Video_Title, channel_name AS Channel_Name FROM videos;"
        cursor.execute("use youtube_data")
        cursor.execute(query1)
        t1 = cursor.fetchall()  # Fetch the results to clear the result set
        mydb.commit()
        
        # Display the results using Streamlit
        st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]))

    elif FAQ == '2. Which channels have the most number of videos, and how many videos do they have?':
        query2 = "select Channel_Name as Channel_Name,Video_Count as No_Of_Videos from channels order by Video_Count desc;"
        cursor.execute("use youtube_data")
        cursor.execute(query2)
        t2 = cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))
        mydb.commit()

    elif FAQ == '3. What are the top 10 most viewed videos and their respective channels?':
        query3 = "Select video_title as Video_Title, view_count as Views, channel_name as Channel_Name from videos WHERE view_count IS NOT NULL ORDER BY view_count DESC LIMIT 10;"
        cursor.execute("use youtube_data")
        cursor.execute(query3)
        t3 = cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=["Video Title","Views","Channel Name"]))
        mydb.commit()
        
    elif FAQ == '4. How many comments were made on each video, and what are their corresponding video names?':
        query4 = "select video_title as Video_Title,  comment_count as No_Of_Comments from videos where comment_count is not null;"
        cursor.execute("use youtube_data")
        cursor.execute(query4)
        t4 = cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["Video Title","No Of Comments"]))
        mydb.commit()
        
    elif FAQ == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5 = "select video_title as Video_Title, channel_name as Channel_Name, like_count as Likes_Count from videos where like_count is not null order by like_count desc;"
        cursor.execute("use youtube_data")
        cursor.execute(query5)
        t5 = cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["Video Title","Channel Name","Likes Count"]))
        mydb.commit()
        
    elif FAQ == '6. What is the total number of likes for each video, and what are their corresponding video names?':
        query6 = "select video_title as Video_Title, like_count as Like_count from videos order by like_count desc;"
        cursor.execute("use youtube_data")
        cursor.execute(query6)
        t6 = cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["Video Title","Like Count"]))
        mydb.commit()
        
    elif FAQ == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        query7 = "select Channel_Name as Channel_Name, Channel_Views as Total_Number_Of_Views from channels order by Channel_Views desc;"
        cursor.execute("use youtube_data")
        cursor.execute(query7)
        t7 = cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel Name", "Total Number Of Views"]))
        mydb.commit()
        
    elif FAQ == '8. What are the names of all the channels that have published videos in the year 2022?':
        query8 = "select channel_name as Channel_Name from videos where published_date like '2022%' group by channel_name order by channel_name"
        cursor.execute("use youtube_data")
        cursor.execute(query8)
        t8 = cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=["Channel Names"]))
        mydb.commit()
        
    elif FAQ == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query9 = "select channel_name as Channel_Name, AVG(duration)/60 AS Average_Video_Duration FROM videos GROUP BY channel_name ORDER BY Average_Video_Duration DESC;"
        cursor.execute("use youtube_data")
        cursor.execute(query9)
        t9 = cursor.fetchall()
        st.write(pd.DataFrame(t9, columns=["Channel Name","Average Video Duration"]))
        mydb.commit()
        
    elif FAQ == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10 = "select channel_name as Channel_Name, video_title as Video_Name,comment_count AS Number_Of_Comments FROM videos ORDER BY comment_count DESC LIMIT 10"
        cursor.execute("use youtube_data")
        cursor.execute(query10)
        t10 = cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=["Channel Name","Video Name","No of Comments"]))
        mydb.commit()
