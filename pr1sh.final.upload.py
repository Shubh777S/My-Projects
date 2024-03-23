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
api_key = 'AIzaSyC-qf_tEyJxQdH1BoWzRN6YbfrmgmYXNKc'
youtube = build(api_service_name, api_version,developerKey=api_key)

#streamlit
# Logo
image_path = "./YT.logo.png"
st.image(image_path, width=100)

# SETTING-UP BACKGROUND IMAGE
st.markdown(f""" <style>.stApp {{
                        background:url("https://wallpapers.com/images/high/pastel-abstract-qp6ehh9uvdv982nx.webp");
                        background-size: cover}}
                     </style>""", unsafe_allow_html=True)

# Adding a title to your Streamlit app with custom styling
st.markdown(
    '<h1 style="text-align:center; color:teal; font-size:3em; margin-top: -40px;">YOUTUBE DATA HARVESTING AND WAREHOUSING</h1>',
    unsafe_allow_html=True)

# Sidebar with tabs
selected_tab = st.sidebar.radio(" ", ["Home","Let's Start"])

# Display content based on selected tab
if selected_tab == "Home":
    st.markdown('<h2 style="color: teal; font-size: 1.5em;">Introduction:</h2>', unsafe_allow_html=True)
    st.info("This project focuses on harvesting and warehousing YouTube data using Python, MongoDB, MySQL, and Streamlit. "
            "Streamlit, a powerful Python library, is integrated to provide a user-friendly interface, making it easy for "
            "users to interact with the data. Streamlit's simplicity and efficiency enable quick development of web "
            "applications without the need for extensive frontend knowledge.")

    st.markdown('<h2 style="color: teal; font-size: 1.5em;">Application:</h2>', unsafe_allow_html=True)
    st.info("The application of this project extends to content creators, marketers, and analysts who seek comprehensive "
            "insights into YouTube channel performance. By leveraging the collected data, users can make informed decisions, "
            "analyze trends, and gain a deeper understanding of audience engagement, ultimately enhancing their content strategy "
            "and optimizing their online presence.")
    

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
client = MongoClient("mongodb://localhost:27017/")
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


# Function to retrieve stored channel IDs from MongoDB
def get_channel_ids():
    channel_ids = coll1.distinct("_id")
    return channel_ids

# Function to display dropdown list of channel IDs
def display_channel_dropdown(channel_ids):
    selected_channel_id = st.selectbox("Select Channel ID", channel_ids)
    return selected_channel_id


#now connect all functions to streamlit

channel_id=st.sidebar.text_input('Enter New Channel ID:')
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
# Button to display table of channel IDs and names present in MongoDB
if st.sidebar.button("Channels Stored in MongoDB"):
    existing_channels = coll1.find({}, {"_id": 1, "channel_information.Channel_Name": 1})
    channel_df = pd.DataFrame(existing_channels)
    # Set index starting from 1
    channel_df.index += 1
    st.write(channel_df)
# Retrieve stored channel IDs
channel_ids = get_channel_ids()
# Display dropdown list of channel IDs
selected_channel_id = st.sidebar.selectbox("Select Channel ID", channel_ids)
# Button to display the selected channel ID
if st.sidebar.button("Show Selected Channel ID"):
    if selected_channel_id:
        st.sidebar.write("Selected Channel ID:", selected_channel_id)
    else:
        st.sidebar.write("No channel selected.")

#migrate to MY SQL:
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="laptop")
cursor = mydb.cursor()

def channels_table(client, selected_channel_id):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="laptop",
        database="youtube_data1"
    )
    cursor = mydb.cursor()

    # Check if table exists, if not create it
    cursor.execute("SHOW TABLES LIKE 'channels'")
    result = cursor.fetchone()
    if not result:
        create_query = '''CREATE TABLE channels(
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(80) PRIMARY KEY, 
                        Subscription_Count BIGINT, 
                        Video_Count INT,
                        Channel_Views BIGINT,
                        Playlist_Id VARCHAR(100))'''
        cursor.execute(create_query)
        mydb.commit()

    # Check if the channel ID already exists in the table
    cursor.execute("SELECT Channel_Id FROM channels WHERE Channel_Id = %s", (selected_channel_id,))
    existing_channel = cursor.fetchone()
    if not existing_channel:
        db = client['first_pr_youtube']
        coll1 = db["channel_details"]

        channel_data = coll1.find_one({"_id": selected_channel_id}, {"_id": 0, 'channel_information': 1})
        if channel_data:
            insert_query = '''INSERT INTO channels(Channel_Name, Channel_Id, Subscription_Count, Video_Count, Channel_Views, Playlist_Id)
                            VALUES(%s,%s,%s,%s,%s,%s)'''
            values = (
                channel_data['channel_information']['Channel_Name'],
                channel_data['channel_information']['Channel_Id'],
                channel_data['channel_information']['Subscription_Count'],
                channel_data['channel_information']['Video_Count'],
                channel_data['channel_information']['Channel_Views'],
                channel_data['channel_information']['Playlist_Id']
            )
            cursor.execute(insert_query, values)
            mydb.commit()

# Function to create or update playlists table
import unicodedata

def playlists_table(client, selected_channel_id):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="laptop",
        database="youtube_data1"
    )
    cursor = mydb.cursor()

    # Check if table exists, if not create it
    cursor.execute("SHOW TABLES LIKE 'playlists'")
    result = cursor.fetchone()
    if not result:
        create_query = '''CREATE TABLE playlists(
                        playlist_id VARCHAR(100),
                        playlist_title VARCHAR(255),  # Set initial length to 255
                        published_date TIMESTAMP,
                        total_videos INT,
                        channel_id VARCHAR(100),
                        PRIMARY KEY (playlist_id, channel_id)
                        )'''
        cursor.execute(create_query)
        mydb.commit()

    # Retrieve data from MongoDB collection and insert into MySQL
    db = client['first_pr_youtube']
    coll1 = db["channel_details"]

    pl_list = []
    max_title_length = 255  # Initialize max title length
    for pl_data in coll1.find({"_id": selected_channel_id},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
            # Update max title length if needed
            max_title_length = max(max_title_length, len(pl_data["playlist_information"][i]["playlist_title"]))
    
    # Alter table to adjust column length if needed
    alter_query = f'ALTER TABLE playlists MODIFY playlist_title VARCHAR({max_title_length})'
    cursor.execute(alter_query)
    mydb.commit()

    for row in pl_list:
        # Check if the playlist ID already exists for the channel
        cursor.execute("SELECT playlist_id FROM playlists WHERE playlist_id = %s AND channel_id = %s", (row['playlist_id'], selected_channel_id))
        existing_playlist = cursor.fetchone()
        if not existing_playlist:
            insert_query = '''INSERT INTO playlists(playlist_id, playlist_title, published_date, total_videos, channel_id)
                                    VALUES(%s,%s,%s,%s,%s)'''
            values =(
                    row['playlist_id'],
                    row['playlist_title'],
                    row['published_date'],
                    row['total_videos'],
                    selected_channel_id)
                
            cursor.execute(insert_query, values)
            mydb.commit()

# Function to create or update videos table
def videos_table(client, selected_channel_id):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="laptop",
        database="youtube_data1"
    )
    cursor = mydb.cursor()

    # Check if table exists, if not create it
    cursor.execute("SHOW TABLES LIKE 'videos'")
    result = cursor.fetchone()
    if not result:
        create_query = '''CREATE TABLE videos(
                        channel_name VARCHAR(50),
                        video_id VARCHAR(50) PRIMARY KEY,
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

    db = client['first_pr_youtube']
    coll1 = db["channel_details"]

    video_data = coll1.find_one({"_id": selected_channel_id}, {"_id": 0, "video_information": 1})
    if video_data:
        for video in video_data["video_information"]:
            # Check if the video ID already exists
            cursor.execute("SELECT video_id FROM videos WHERE video_id = %s", (video['video_id'],))
            existing_video = cursor.fetchone()
            if not existing_video:
                insert_query = '''
                    INSERT INTO videos (channel_name, video_id, video_title, video_description, published_date, view_count, like_count, comment_count, thumbnail, duration)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''
                values = (
                    video['channel_name'],
                    video['video_id'],
                    video['video_title'],
                    video['video_description'],
                    video['published_date'],
                    video['view_count'],
                    video['like_count'],
                    video['comment_count'],
                    video['thumbnail'],
                    video['duration']
                )
                cursor.execute(insert_query, values)
                mydb.commit()
def comments_table(client, selected_channel_id):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="laptop",
        database="youtube_data1"
    )
    cursor = mydb.cursor()

    # Check if table exists, if not create it
    cursor.execute("SHOW TABLES LIKE 'comments'")
    result = cursor.fetchone()
    if not result:
        create_query = '''CREATE TABLE comments(
                        Comment_ID VARCHAR(50) PRIMARY KEY,
                        Video_ID VARCHAR(50),
                        Author VARCHAR(50),
                        Text TEXT,
                        Published_At DATETIME
                        )'''
        cursor.execute(create_query)
        mydb.commit()

    db = client['first_pr_youtube']
    coll1 = db["channel_details"]

    comment_data = coll1.find_one({"_id": selected_channel_id}, {"_id": 0, "comment_information": 1})
    if comment_data:
        for comment in comment_data["comment_information"]:
            # Check if the comment ID already exists
            cursor.execute("SELECT Comment_ID FROM comments WHERE Comment_ID = %s", (comment['Comment_ID'],))
            existing_comment = cursor.fetchone()
            if not existing_comment:
                insert_query = '''
                    INSERT INTO comments (Comment_ID, Video_ID, Author, Text, Published_At)
                    VALUES (%s, %s, %s, %s, %s)
                '''
                values = (
                    comment['Comment_ID'],
                    comment['Video_ID'],
                    comment['Author'],
                    comment['Text'],
                    comment['Published_At']
                )
                cursor.execute(insert_query, values)
                mydb.commit()

# Function to create tables and migrate data
def tables():
    channels_table(client, selected_channel_id)
    playlists_table(client, selected_channel_id)
    videos_table(client, selected_channel_id)
    comments_table(client, selected_channel_id)
    return "Tables Created successfully"


# Define MySQL connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="laptop",
    database="youtube_data1"
)
cursor = mydb.cursor()

def show_channels_table():
    cursor.execute("SELECT * FROM channels")
    channels_data = cursor.fetchall()
    channels_df = pd.DataFrame(channels_data, columns=[desc[0] for desc in cursor.description])
    channels_df.index = channels_df.index + 1
    return channels_df

def show_playlists_table():
    cursor.execute("SELECT * FROM playlists")
    playlists_data = cursor.fetchall()
    playlists_df = pd.DataFrame(playlists_data, columns=[desc[0] for desc in cursor.description])
    playlists_df.index = playlists_df.index + 1
    return playlists_df

def show_videos_table():
    cursor.execute("SELECT * FROM videos")
    videos_data = cursor.fetchall()
    videos_df = pd.DataFrame(videos_data, columns=[desc[0] for desc in cursor.description])
    videos_df.index = videos_df.index + 1
    return videos_df

def show_comments_table():
    cursor.execute("SELECT * FROM comments")
    comments_data = cursor.fetchall()
    comments_df = pd.DataFrame(comments_data, columns=[desc[0] for desc in cursor.description])
    comments_df.index = comments_df.index + 1
    return comments_df

  #execute streamlit to migrate in sql.

if selected_channel_id:
    migrate_button_clicked = st.sidebar.button("Migrate to SQL")

    if migrate_button_clicked:
        try:
            display = tables()
            st.success(display)
        except Exception as e:
            st.sidebar.write("Error occurred during migration:", e)

    # Now, you can display other options only if the "Migrate to SQL" button is clicked
    show = st.sidebar.radio('Select from below', ('None', 'Show Table', 'FAQ'))

    FAQ = None 

    if show == "Show Table":
        show_table = st.radio("Select Table", ("Channels", "Playlists", "Videos", "Comments"))
        if show_table == "Channels":
            st.dataframe(show_channels_table())
        elif show_table == "Playlists":
            st.dataframe(show_playlists_table())
        elif show_table == "Videos":
            st.dataframe(show_videos_table())
        elif show_table == "Comments":
            st.dataframe(show_comments_table())

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
        cursor.execute("use youtube_data1")
        cursor.execute(query1)
        t1 = cursor.fetchall()  # Fetch the results to clear the result set
        mydb.commit()

        # Display the results using Streamlit
        st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"], index=range(1, len(t1) + 1)))

    elif FAQ == '2. Which channels have the most number of videos, and how many videos do they have?':
        query2 = "SELECT Channel_Name AS Channel_Name,Video_Count AS No_Of_Videos FROM channels ORDER BY Video_Count desc;"
        cursor.execute("use youtube_data1")
        cursor.execute(query2)
        t2 = cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name", "No Of Videos"], index=range(1, len(t2) + 1)))
        mydb.commit()

    elif FAQ == '3. What are the top 10 most viewed videos and their respective channels?':
        query3 = "Select video_title as Video_Title, view_count as Views, channel_name as Channel_Name from videos WHERE view_count IS NOT NULL ORDER BY view_count DESC LIMIT 10;"
        cursor.execute("use youtube_data1")
        cursor.execute(query3)
        t3 = cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=["Video Title", "Views", "Channel Name"], index=range(1, len(t3) + 1)))
        mydb.commit()

    elif FAQ == '4. How many comments were made on each video, and what are their corresponding video names?':
        query4 = "select video_title as Video_Title,  comment_count as No_Of_Comments from videos where comment_count is not null;"
        cursor.execute("use youtube_data1")
        cursor.execute(query4)
        t4 = cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["Video Title", "No Of Comments"], index=range(1, len(t4) + 1)))
        mydb.commit()

    elif FAQ == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5 = "select video_title as Video_Title, channel_name as Channel_Name, like_count as Likes_Count from videos where like_count is not null order by like_count desc;"
        cursor.execute("use youtube_data1")
        cursor.execute(query5)
        t5 = cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["Video Title", "Channel Name", "Likes Count"], index=range(1, len(t5) + 1)))
        mydb.commit()

    elif FAQ == '6. What is the total number of likes for each video, and what are their corresponding video names?':
        query6 = "select video_title as Video_Title, like_count as Like_count from videos order by like_count desc;"
        cursor.execute("use youtube_data1")
        cursor.execute(query6)
        t6 = cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["Video Title", "Like Count"], index=range(1, len(t6) + 1)))
        mydb.commit()

    elif FAQ == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        query7 = "select Channel_Name as Channel_Name, Channel_Views as Total_Number_Of_Views from channels order by Channel_Views desc;"
        cursor.execute("use youtube_data1")
        cursor.execute(query7)
        t7 = cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel Name", "Total Number Of Views"], index=range(1, len(t7) + 1)))
        mydb.commit()

    elif FAQ == '8. What are the names of all the channels that have published videos in the year 2022?':
        query8 = "select channel_name as Channel_Name from videos where published_date like '2022%' group by channel_name order by channel_name"
        cursor.execute("use youtube_data1")
        cursor.execute(query8)
        t8 = cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=["Channel Names"], index=range(1, len(t8) + 1)))
        mydb.commit()

    elif FAQ == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query9 = "select channel_name as Channel_Name, AVG(duration)/60 AS Average_Video_Duration FROM videos GROUP BY channel_name ORDER BY Average_Video_Duration DESC;"
        cursor.execute("use youtube_data1")
        cursor.execute(query9)
        t9 = cursor.fetchall()
        st.write(pd.DataFrame(t9, columns=["Channel Name", "Average Video Duration"], index=range(1, len(t9) + 1)))
        mydb.commit()

    elif FAQ == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10 = "SELECT channel_name AS Channel_Name, video_title AS Video_Name, comment_count AS Number_Of_Comments FROM videos ORDER BY comment_count DESC LIMIT 10;"
        cursor.execute("use youtube_data1")
        cursor.execute(query10)
        t10 = cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=["Channel Name", "Video Name", "No of Comments"], index=range(1, len(t10) + 1)))
        mydb.commit()





































































