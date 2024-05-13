import pandas as pd
import streamlit as st
import mysql.connector
import pymongo
from googleapiclient.discovery import build
import plotly.express as px
with st.sidebar:
    selected = st.radio("Choose here",
        ("Home","Get Data & Transform","Show Tables & SQL Query","Insights")
    )

# Bridging a connection with MongoDB  and Creating a new database(youtube)
client = pymongo.MongoClient("mongodb+srv://shivaniind11:IIB1QcLhxlTBYQdP@cluster0.acqsskr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["youtube_det"]

# CONNECTING WITH MYSQL DATABASE
mydb = mysql.connector.connect(host="localhost",#your sql connection requirement
                               user="root",
                               password="Aishini@24",
                               database="youtube_details"
                               )
mycursor = mydb.cursor(buffered=True)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyALVLAWIPo61yGt2T-JyyOIOaWAFu4OJKI"
youtube = build('youtube', 'v3', developerKey=api_key)


# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part='snippet,contentDetails,statistics',
                                       id=channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id=channel_id[i],
                    Channel_name=response['items'][i]['snippet']['title'],
                    Playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers=response['items'][i]['statistics']['subscriberCount'],
                    Views=response['items'][i]['statistics']['viewCount'],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    Description=response['items'][i]['snippet']['description'],
                    Country=response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data



# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []

    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i + 50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                 Channel_id=video['snippet']['channelId'],
                                 Video_id=video['id'],
                                 Title=video['snippet']['title'],
                                 # Tags = video['snippet'].get('tags'),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 Views=video['statistics']['viewCount'],
                                 Likes=video['statistics'].get('likeCount'),
                                 Comments=video['statistics'].get('commentCount'),
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=100,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data



# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


# HOME PAGE
if selected == "Home":
   
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.markdown("## :green[Domain] : Social Media ")
    st.markdown(
        "## :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")

    # EXTRACT AND TRANSFORM PAGE
if selected == "Get Data & Transform":
    tab1, tab2 = st.tabs(["GET DATA ", "TRANSFORM TO SQL "])

    # GET DATA TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input(
            "Hint : Go to the YouTube website (https://www.youtube.com) > channel's home page > Right click > View page source > Find channel_id").split(
            ',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)
        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)
            


                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d += get_comments_details(i)
                    return com_d


                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)

            

                st.success("Upload to MogoDB successful !!")
                

                # TRANSFORM TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")

        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)


        def insert_into_channels():
            collections = db.channel_details
            query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""

            # Check if the channel details already exist in the database
            mycursor.execute("SELECT * FROM channels WHERE Channel_name = %s", (user_inp,))
            existing_channel = mycursor.fetchone()

            if existing_channel:
                st.error("Channel details already exist in MySQL!")
            else:
                # Insert channel details into the database
                for i in collections.find({"Channel_name": user_inp}, {'_id': 0}):
                    mycursor.execute(query, tuple(i.values()))
                    mydb.commit()
                st.success("Channel details inserted into MySQL successfully!")



        def insert_into_videos():
            collections1 = db.video_details
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collections1.find({"Channel_name": user_inp}, {'_id': 0}):
                mycursor.execute(query1, tuple(i.values()))
                mydb.commit()


        def insert_into_comments():
            collections1 = db.video_details
            collections2 = db.comments_details
            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"Channel_name": user_inp}, {'_id': 0}):
                for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):
                    mycursor.execute(query2, tuple(i.values()))
                    mydb.commit()
        
        

        if st.button("Submit"):
            try:
                insert_into_channels()
                insert_into_videos()
                insert_into_comments()

                st.success("Transformation to MySQL Successful !!")
                

            except:
                st.error("Channel details already transformed !!")
   

# VIEW PAGE
if selected == "Show Tables & SQL Query":
    tab3, tab4 = st.tabs([" Show Tables ", "SQL Query"])


    with tab3:# Display channel table
        st.header("Channel Table")
        mycursor.execute("SELECT * FROM channels")
        channel_data = mycursor.fetchall()
        channel_df = pd.DataFrame(channel_data, columns=[desc[0] for desc in mycursor.description])
        st.write(channel_df)
        
        # Display comment table
        st.header("Comment Table")
        mycursor.execute("SELECT * FROM comments")
        comment_data = mycursor.fetchall()
        comment_df = pd.DataFrame(comment_data, columns=[desc[0] for desc in mycursor.description])
        st.write(comment_df)

        # Display videos table
        st.header("Videos Table")
        mycursor.execute("SELECT * FROM videos")
        video_data = mycursor.fetchall()
        video_df = pd.DataFrame(video_data, columns=[desc[0] for desc in mycursor.description])
        st.write(video_df)


    with tab4:
        st.write("## :green[Take your pick from the questions below]")
        questions = st.selectbox('Questions',
                                ['1. What are the names of all the videos and their corresponding channels?',
                                '2. Which channels have the most number of videos, and how many videos do they have?',
                                '3. What are the top 10 most viewed videos and their respective channels?',
                                '4. How many comments were made on each video, and what are their corresponding video names?',
                                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8. What are the names of all the channels that have published videos in the year 2022?',
                                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

        if questions == '1. What are the names of all the videos and their corresponding channels?':
            mycursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name
                                FROM videos
                                ORDER BY channel_name""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)

        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                                FROM channels
                                ORDER BY total_videos DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)


        elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                                FROM videos
                                ORDER BY views DESC
                                LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)


        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            mycursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                                FROM videos AS a
                                LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                                FROM comments GROUP BY video_id) AS b
                                ON a.video_id = b.video_id
                                ORDER BY b.Total_Comments DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)

        elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                                FROM videos
                                ORDER BY likes DESC
                                LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)


        elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            mycursor.execute("""SELECT title AS Title, likes AS Likes_Count
                                FROM videos
                                ORDER BY likes DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)

        elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                                FROM channels
                                ORDER BY views DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)


        elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
            mycursor.execute("""SELECT channel_name AS Channel_Name
                                FROM videos
                                WHERE published_date LIKE '2022%'
                                GROUP BY channel_name
                                ORDER BY channel_name""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)

        elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name,
                                AVG(duration)/60 AS "Average_Video_Duration (mins)"
                                FROM videos
                                GROUP BY channel_name
                                ORDER BY AVG(duration)/60 DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)



        elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                                FROM videos
                                ORDER BY comments DESC
                                LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write(df)

if selected == "Insights":


        # Connect to MySQL database
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Aishini@24",
            database="youtube_details"
        )
        mycursor = mydb.cursor()

        # Fetch data from MySQL table
        mycursor.execute("SELECT channel_name, subscription_count, views, total_videos FROM channels")
        data = mycursor.fetchall()

        # Extracting data
        channel_names = [row[0] for row in data]
        subscription_counts = [row[1] for row in data]
        views = [row[2] for row in data]
        total_videos = [row[3] for row in data]

        # Creating a DataFrame for Plotly Express
        df = pd.DataFrame({'Channel Name': channel_names, 
                        'Subscription Count': subscription_counts,
                        'Views': views,
                        'Total Videos': total_videos})

        # Plotting subscription count using Plotly Express
        fig_subscription = px.bar(df, x='Channel Name', y='Subscription Count', 
                                title='Subscription Count by Channel',
                                labels={'Subscription Count': 'Count', 'Channel Name': 'Channel Name'},
                                width=500, height=500)  # Set figure size
        fig_subscription.update_traces(marker_color='green')  # Set subscription count bars to green

        # Plotting views using Plotly Express
        fig_views = px.bar(df, x='Channel Name', y='Views', 
                        title='Views by Channel',
                        labels={'Views': 'Count', 'Channel Name': 'Channel Name'},
                        width=500, height=500)  # Set figure size
        fig_views.update_traces(marker_color='blue')  # Set views bars to blue

        # Plotting total videos using Plotly Express
        fig_total_videos = px.bar(df, x='Channel Name', y='Total Videos', 
                                title='Total Videos by Channel',
                                labels={'Total Videos': 'Count', 'Channel Name': 'Channel Name'},
                                width=500, height=500)  # Set figure size
        fig_total_videos.update_traces(marker_color='orange')  # Set total videos bars to orange

        # Display Insights page
        st.title("Insights")

        # Display subscription count plot
        st.plotly_chart(fig_subscription)

        # Display views plot
        st.plotly_chart(fig_views)

        # Display total videos plot
        st.plotly_chart(fig_total_videos)
