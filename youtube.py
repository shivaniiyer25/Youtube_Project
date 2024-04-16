from googleapiclient.discovery import build
import pymongo
import pandas as pd
import streamlit as st
import mysql.connector as sql
import logging


def Api_connect():
    api_Id = "AIzaSyAL0jnLxUri7qsr2-T6F4p4O1LQK24rrXc"
    api_service_name = "youtube"
    api_version= "v3"

    youtube = build(api_service_name,api_version, developerKey= api_Id)

    return youtube
youtube = Api_connect()




def channel_info(channel_id):
    request = youtube.channels().list(
                   part = "snippet,ContentDetails,statistics",
                   id = channel_id
    ) 
    response = request.execute()     

    response= youtube.channels().list(
        part = "snippet,ContentDetails,statistics",
        id =channel_id
    )
    response = request.execute()

    for i in response['items']:
        data = dict(Channel_Name = i["snippet"]["title"],
                    Channel_Id = i["id"],
                    Subscribers= i["statistics"]["subscriberCount"],
                    Views = i["statistics"]["viewCount"],
                    Total_Videos = i["statistics"]["videoCount"],
                    Channel_Description = i["snippet"]["description"],
                    Playlist_Id = i["contentDetails"]["relatedPlaylists"]["uploads"]) 
        return data
    

def video_ids(channel_id):
    video_ids =[]
    response = youtube.channels().list(id = channel_id,
                                       part = 'contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                                                 part = 'snippet',
                                                 playlistId = Playlist_Id,
                                                 maxResults = 50,
                                                 pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
        return video_ids
    

def video_info(video_ids):
    video_data =[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part = "snippet, contentDetails, statistics",
            id = video_id
        )
        response = request.execute()

        for item in response['items']:
            data = dict( Channel_Name = item ['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_Date = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        Comments= item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status= item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data  



def comment_info(video_ids):
    Comment_data = []
    try:
        for video_id in video_ids:
            print("Fetching comments for video:", video_id)
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100
            )
            response = request.execute()

            for item in response.get('items', []):
                data = {
                    "Comment_Id": item['snippet']['topLevelComment']['id'],
                    "Video_Id": item['snippet']['topLevelComment']['snippet']['videoId'],
                    "Comment_Text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "Comment_Author": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    "Comment_Published": item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                Comment_data.append(data)
    except Exception as e:
        print("An error occurred:", e)
    return Comment_data

comment_details = comment_info(video_ids)

def playlist_details(channel_id):
    next_page_token = None
    Playlist_data = []
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                        Title=item['snippet']['title'],
                        Channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],  
                        Video_Count=item['contentDetails']['itemCount'])
            Playlist_data.append(data)
        next_page_token = response.get('nextPageToken')  
        if next_page_token is None:
            break
    return Playlist_data


import pymongo
client = pymongo.MongoClient("mongodb+srv://shivaniiyer302025:Aishini24@cluster0.psd199w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["Youtube_data"]

def channel_details(channel_id):
    ch_details = channel_info(channel_id)
    pl_details = playlist_details(channel_id)
    vi_ids = video_ids(channel_id)
    vi_details = video_info(vi_ids)
    com_details = comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information": ch_details, "playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    return "uploaded successfully"


#Table creation
client = pymongo.MongoClient("mongodb+srv://shivaniiyer302025:Aishini24@cluster0.psd199w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["Youtube_data"]
import pandas as pd
import mysql.connector as sql

def playlist_table():
    mydb = sql.connect(
        host="localhost",
        user="root",
        password="Aishini@24",
        database="youtube_data",
        port = "3306"
    )
    cursor = mydb.cursor()

    drop_query ='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists playlists (Playlists_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100)
                                                            PublishedAt timestamp
                                                            Video_Count int
                                                            )'''
    cursor.execute(create_query)
    mydb.commit()
    
    pl_list =[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1 = pd.DataFrame(pl_list)
        





    for index, row in df1.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                                                Title
                                                Channel_Id,
                                                Channel_Name,
                                                PiblisheAt,
                                                Video_Count,
                                            
                                            Values(%s,%s,%s,%s,%s,%s)'''
        values =(row['Playlist_Id'],
                 row['Title'],
                 row['Channel_Id'],
                 row['Channel_Name'],
                 row['PublishedAt'],
                 row['Video_Count'],)
        

        cursor.execute(insert_query,values)
        mydb.commit()
        




# Configure logging
logging.basicConfig(level=logging.DEBUG)

def playlist_table():
    try:
        # MySQL connection
        mydb = sql.connect(
            host="localhost",
            user="root",
            password="Aishini@24",
            database="youtube_data",
            port="3306"
        )
        cursor = mydb.cursor()

        # Drop table if exists
        drop_query = '''DROP TABLE IF EXISTS playlists'''
        cursor.execute(drop_query)
        mydb.commit()

        # Create table with 'PublishedAt' field
        create_query = '''CREATE TABLE IF NOT EXISTS playlists (
                            Playlist_Id VARCHAR(100) PRIMARY KEY,
                            Title VARCHAR(200),
                            Channel_Id VARCHAR(100),
                            Channel_Name VARCHAR(100),
                            PublishedAt TIMESTAMP,
                            Video_Count INT
                        )'''
        cursor.execute(create_query)
        mydb.commit()

        # MongoDB connection
        client = pymongo.MongoClient("mongodb+srv://shivaniiyer302025:Aishini24@cluster0.psd199w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        db = client["Youtube_data"]
        coll1 = db["channel_details"]

        # Retrieve playlist data from MongoDB
        pl_list = []
        for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
            for playlist in pl_data.get("playlist_information", []):
                pl_list.append(playlist)

        df1 = pd.DataFrame(pl_list)

        # Insert playlist data into MySQL table, checking for duplicates
        for index, row in df1.iterrows():
            playlist_id = row['Playlist_Id']
            # Check if Playlist_Id already exists in the table
            cursor.execute("SELECT * FROM playlists WHERE Playlist_Id = %s", (playlist_id,))
            existing_playlist = cursor.fetchone()
            if existing_playlist:
                logging.warning(f"Playlist with ID {playlist_id} already exists. Skipping insertion.")
            else:
                insert_query = '''INSERT INTO playlists (Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count)
                                  VALUES (%s, %s, %s, %s, %s, %s)'''
                values = (row['Playlist_Id'], row['Title'], row['Channel_Id'], row['Channel_Name'], row.get('PublishedAt'), row['Video_Count'])
                cursor.execute(insert_query, values)
                mydb.commit()

        # Close MySQL connection
        cursor.close()
        mydb.close()

        logging.info("Playlist table creation and data insertion successful.")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

# Call the function to execute the process
playlist_table()


def videos_table():
    # Connect to MySQL database
    mydb = sql.connect(
        host="localhost",
        user="root",
        password="Aishini@24",
        database="youtube_data",
        port="3306"
    )
    cursor = mydb.cursor()

    # Drop existing table if exists
    drop_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create new table
    create_query = '''CREATE TABLE IF NOT EXISTS videos (
                        Channel_Name VARCHAR(100),
                        Channel_Id VARCHAR(100),
                        Video_Id VARCHAR(30) PRIMARY KEY,
                        Title VARCHAR(150),
                        Tags TEXT,
                        Thumbnail VARCHAR(200),
                        Description TEXT,
                        Views BIGINT,
                        Likes BIGINT,
                        Comments INT,
                        Favorite_Count INT,
                        Definition VARCHAR(10),
                        Caption_Status VARCHAR(50)
                    )'''
    cursor.execute(create_query)
    mydb.commit()

    # MongoDB connection
    client = pymongo.MongoClient("mongodb+srv://shivaniiyer302025:Aishini24@cluster0.psd199w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client["Youtube_data"]
    coll1 = db["channel_details"]

    vi_list = []

    # Retrieve video information from MongoDB into a list
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for video in vi_data.get("video_information", []):
            vi_list.append(video)

    df2 = pd.DataFrame(vi_list)

    # Insert data into MySQL table
    for index, row in df2.iterrows():
        # Convert list-type columns to strings
        row['Tags'] = ','.join(row['Tags']) if isinstance(row['Tags'], list) else row['Tags']
        row['Description'] = ','.join(row['Description']) if isinstance(row['Description'], list) else row['Description']

        insert_query = '''INSERT INTO videos (
                            Channel_Name,
                            Channel_Id,
                            Video_Id,
                            Title,
                            Tags,
                            Thumbnail,
                            Description,
                            Views,
                            Likes,
                            Comments,
                            Favorite_Count,
                            Definition,
                            Caption_Status
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                  row['Channel_Id'],
                  row['Video_Id'],
                  row['Title'],
                  row['Tags'],
                  row['Thumbnail'],
                  row['Description'],
                  row['Views'],
                  row['Likes'],
                  row['Comments'],
                  row['Favorite_Count'],
                  row['Definition'],
                  row['Caption_Status']
                  )
        cursor.execute(insert_query, values)
        mydb.commit()

    cursor.close()
    mydb.close()

# Call the function to execute the process
videos_table()


import pandas as pd
import mysql.connector as sql
import pymongo
from datetime import datetime

def convert_datetime(datetime_str):
    # Parse the ISO 8601 datetime string
    dt = datetime.fromisoformat(datetime_str[:-1])  # Remove 'Z' at the end
    # Format the datetime in MySQL datetime format
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def comments_table():
    # Connect to MySQL database
    mydb = sql.connect(
        host="localhost",
        user="root",
        password="Aishini@24",
        database="youtube_data",
        port="3306"
    )
    cursor = mydb.cursor()

    # Drop existing table if exists
    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create new table
    create_query = '''CREATE TABLE IF NOT EXISTS comments (
                                                           Comment_Id varchar(100) primary key,
                                                           Video_Id varchar(50),
                                                           Comment_Text text,
                                                           Comment_Author varchar(150),
                                                           Comment_Published datetime
                                                           )'''
    cursor.execute(create_query)
    mydb.commit()
    
    # MongoDB connection
    client = pymongo.MongoClient("mongodb+srv://shivaniiyer302025:Aishini24@cluster0.psd199w.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    coll1 = db["channel_details"]

    com_list =[]
    
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        # Convert the datetime format
        comment_published = convert_datetime(row['Comment_Published'])
        
        insert_query = '''INSERT INTO comments (
                                                Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published
                                            ) VALUES (%s,%s,%s,%s,%s)'''
        values = (row['Comment_Id'],
                  row['Video_Id'],
                  row['Comment_Text'],
                  row['Comment_Author'],
                  comment_published)  # Use the converted datetime value
        cursor.execute(insert_query, values)
        mydb.commit()     

    cursor.close()
    mydb.close()

comments_table()


def tables():
    channel_details()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables created successfully"


def show_channels_table():
    ch_list =[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
            for playlist in pl_data.get("playlist_information", []):
                pl_list.append(playlist)

    df1 = st.dataframe(pl_list)

    return df1


def show_videos_table():
    vi_list =[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for video in vi_data.get("video_information", []):
            vi_list.append(video)
        df2 = st.dataframe(vi_list)

        return df2


def show_comment_table():
    com_list =[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)


with st.sidebar:
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.header("Skills Aquired")
    st.caption("Python Scripting")
    st.caption("Data Collection ")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data management using MongoDB and MYSQL")

channel_id = st.text_input("Enteer the channel ID")


if st.button("collect and store data"):
    ch_ids = []
    db = client["Youtube)data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel already exists")
    else:
        insert = channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
        Table = tables()
        st.success(Table)

show_tables = st.radio("SELECT THE TABLE FOR VIEWING",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_tables == "CHANNELS":
        show_channels_table()
    
elif show_tables == "PLAYLISTS":
        show_playlists_table()

elif show_tables == "VIDEOS":
        show_videos_table()

elif show_tables == "COMMENTS":
        show_comment_table()



# SQL CONNECT
import mysql.connector as sql
import pandas as pd
import streamlit as st

# Establish MySQL connection
mydb = sql.connect(
    host="localhost",
    user="root",
    password="Aishini@24",
    database="youtube_data",
    port="3306"
)
cursor = mydb.cursor()

# Select the question from the dropdown
question = st.selectbox("Select your question", [
    "1. All the videos and the channel name",
    "2. Channels with most number of videos",
    "3. 10 most viewed videos",
    "4. Comments in each video",
    "5. Videos with highest likes",
    "6. Likes of all videos",
    "7. Views of each channel",
    "8. Videos published in the year of 2022",
    "9. Average duration of all videos in each channel",
    "10. Videos with highest number of comments"
])

# Execute SQL queries based on selected question
if question == "1. All the videos and the channel name":
    query1 = '''SELECT title AS videos, channel_name AS channelname FROM videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df1 = pd.DataFrame(t1, columns=["video title", "channel name"])
    st.write(df1)

elif question == "2. Channels with most number of videos":
    query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos FROM channels ORDER BY total_videos DESC'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "No of videos"])
    st.write(df2)

elif question == "3. 10 most viewed videos":
    query3 = '''SELECT views AS views, channel_name AS channelname, title AS videotitle FROM videos WHERE views IS NOT NULL ORDER BY views DESC LIMIT 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "videotitle"])
    st.write(df3)

elif question == "4. Comments in each video":
    query4 = '''SELECT comments AS no_comments, title AS videotitle FROM videos WHERE comments IS NOT NULL'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of comments", "videotitle"])
    st.write(df4)

elif question == "5. Videos with highest likes":
    query5 = '''SELECT title AS videotitle, channel_name AS channelname, likes AS likecount FROM videos WHERE likes IS NOT NULL ORDER BY likes DESC'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["videotitle", "channelname", "likecount"])
    st.write(df5)

elif question == "6. Likes of all videos":
    query6 = '''SELECT likes AS likecount, title AS videotitle FROM videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6)

elif question == "7. Views of each channel":
    query7 = '''SELECT channel_name AS channelname ,views AS totalviews FROM channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channel name", "totalviews"])
    st.write(df7)

elif question == "8. Videos published in the year of 2022":
    query8 = '''SELECT title AS video_title, published_date AS videorelease, channel_name AS channelname FROM videos WHERE YEAR(published_date) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8)

elif question == "9. Average duration of all videos in each channel":
    query9 = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration FROM videos GROUP BY channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

    T9 = []
    for index, row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df9 = pd.DataFrame(T9)
    st.write(df9)

elif question == "10. Videos with highest number of comments":
    query10 = '''SELECT title AS videotitle, channel_name AS channelname, comments AS comments FROM videos WHERE comments IS NOT NULL ORDER BY comments DESC'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title", "channel name", "comments"])
    st.write(df10)


