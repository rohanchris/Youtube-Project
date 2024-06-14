Youtube Data Harvestinng and Warehousing
### Step-by-Step Explanation

####  Imports and API Key Setup

```python
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
```

- **Imports**: The required libraries are imported for API calls (`googleapiclient`), database operations (`pymongo`, `psycopg2`), data manipulation (`pandas`), and creating a web app (`streamlit`).

####  API Connection

```python
def Api_connect():
    Api_Id="YOUR_API_KEY"
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name, api_version, developerKey=Api_Id)

    return youtube

youtube=Api_connect()
```

- **API Key**: The `Api_connect` function uses the YouTube Data API to connect using the provided API key.
- **Service Setup**: The `build` function initializes the YouTube API client.

####  Get Channel Info

```python
def get_channel_info(channel_id):
    response = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    ).execute()

    for i in response["items"]:
        data = dict(
            Channel_Name=i["snippet"]["title"],
            Channel_Id=i["id"],
            Subscription_Count=i["statistics"]['subscriberCount'],
            Channel_Views=i['statistics']['viewCount'],
            Channel_Description=i['snippet']['description'],
            Total_videos=i['statistics']['videoCount'],
            Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
        )

        return data
```

- **Fetch Data**: The function fetches channel details using the `channels().list` method.
- **Data Extraction**: Extracts relevant information (channel name, ID, subscriber count, etc.) and stores it in a dictionary.

####  Get Video IDs

```python
def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()

    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']   
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_Id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids
```

- **Fetch Playlist**: Retrieves the playlist ID containing all uploads from the channel.
- **Loop Through Playlist**: Uses pagination to get all video IDs from the playlist.
- **Collect Video IDs**: Appends each video ID to a list and returns it.

####  Get Video Info

```python
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        response = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        ).execute()
        
        for i in response['items']:
            data = dict(
                Channel_Name=i['snippet']['channelTitle'],
                Channel_Id=i['snippet']['channelId'],
                Video_Id=i['id'],
                Video_Name=i['snippet']['title'],
                Video_Description=i['snippet']['description'],
                Tags=i.get('tags'),
                PublishedAt=i['snippet']['publishedAt'],
                View_Count=i['statistics']['viewCount'],
                Like_Count=i['statistics']['likeCount'],
                Favorite_Count=i['statistics']['favoriteCount'],
                Comment_Count=i['statistics']['commentCount'],
                Duration=i['contentDetails']['duration'],
                Thumbnail=i['snippet']['thumbnails']['default']['url'],
                Caption_Status=i['contentDetails']['caption']
            )
            video_data.append(data)

    return video_data
```

- **Fetch Video Details**: For each video ID, the function retrieves details using `videos().list`.
- **Data Extraction**: Extracts relevant video details and stores them in a dictionary.
- **Collect Data**: Appends each video's data to a list and returns it.

####  Get Playlist Info

```python
def get_playlist_info(channel_id):
    next_page_token = None
    All_data = []

    while True:
        response = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for i in response['items']:
            data = dict(
                Playlist_Id=i['id'],
                Title=i['snippet']['title'],
                Channel_Id=i['snippet']['channelId'],
                Channel_Name=i['snippet']['channelTitle'],
                PublishedAt=i['snippet']['publishedAt'],
                Video_Count=i['contentDetails']['itemCount']
            )
            All_data.append(data)
        
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return All_data
```

- **Fetch Playlists**: Retrieves all playlists of the channel.
- **Loop Through Playlists**: Uses pagination to fetch playlist details.
- **Collect Data**: Appends each playlist's data to a list and returns it.

### Summary

- **API Integration**: The code integrates with YouTube Data API to fetch channel, video, and playlist details.
- **Data Extraction**: It extracts detailed information and structures it into dictionaries and lists.
- **Pagination Handling**: Efficiently handles pagination for large data sets using `nextPageToken`.
- **Data Usage**: This data can be used for further analysis, visualization, or storage in databases.

### New Functionality Overview

1. **Get Comment Info**: Fetches comments for videos.
2. **Upload to MongoDB**: Stores YouTube data in a MongoDB collection.
3. **SQL Tables Creation**: Creates SQL tables and inserts YouTube data into them.

####  Get Comment Info

```python
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100
            ).execute()
            
            for i in response['items']:
                data = dict(
                    Comment_Id=i['snippet']['topLevelComment']['id'],
                    Comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=i['snippet']['topLevelComment']['snippet']['publishedAt'],
                    Video_Id=i['snippet']['topLevelComment']['snippet']['videoId']
                )
                
                comment_data.append(data)
                
    except Exception as e:
        print(f"An error occurred: {e}")
        
    return comment_data
```

- **Fetch Comments**: The function retrieves comments for a list of video IDs.
- **Data Extraction**: Extracts details about each comment (ID, text, author, publish date, video ID) and stores them in a dictionary.
- **Error Handling**: Uses a try-except block to handle potential errors gracefully.

####  Upload to MongoDB

```python
client = pymongo.MongoClient("mongodb+srv://your_mongodb_connection_string")
db = client["Youtube_data"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    pl_details = get_playlist_info(channel_id)
    comm_details = get_comment_info(vi_ids)

    collection1 = db["channel_details"]
    collection1.insert_one({
        "channel_info": ch_details,
        "video_info": vi_details,
        "playlist_info": pl_details,
        "comment_info": comm_details
    })

    return "uploaded Successfully"
```

- **MongoDB Client**: Connects to a MongoDB instance.
- **Data Collection**: Gathers channel, video, playlist, and comment data.
- **Insert Data**: Inserts the collected data into the `channel_details` collection in MongoDB.

####  SQL Tables Creation and Data Insertion

```python
def channel_tables(channel_name_s):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    create_query = '''
    CREATE TABLE IF NOT EXISTS channels (
        Channel_Name VARCHAR(255),
        Channel_Id VARCHAR(255) PRIMARY KEY,
        Subscription_Count BIGINT,
        Channel_Views BIGINT,
        Channel_Description TEXT,
        Total_videos INT,
        Playlist_Id VARCHAR(255)
    )
    '''
    
    cursor.execute(create_query)
    mydb.commit()

    solo_channel_detail = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_info.Channel_Name": channel_name_s}, {"_id": 0}):
        solo_channel_detail.append(ch_data["channel_info"])
    
    df_solo_channel_detail = pd.DataFrame(solo_channel_detail)

    for index, row in df_solo_channel_detail.iterrows():
        insert_query = '''
        INSERT INTO channels (
            Channel_Name,
            Channel_Id,
            Subscription_Count,
            Channel_Views,
            Channel_Description,
            Total_videos,
            Playlist_Id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['Channel_Views'],
            row['Channel_Description'],
            row['Total_videos'],
            row['Playlist_Id']
        )
        
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            meow = f"Your Provided Channel Name {channel_name_s} already exists. Error: {e}"
            return meow
```

- **Database Connection**: Connects to a PostgreSQL database.
- **Table Creation**: Creates a `channels` table if it doesn't already exist.
- **Data Extraction**: Fetches the channel details from MongoDB.
- **Data Insertion**: Inserts the fetched data into the `channels` table in PostgreSQL.
- **Error Handling**: Handles errors during the insert operation, such as duplicate entries.

### Summary

- **Comment Data**: The `get_comment_info` function retrieves comments for videos.
- **Data Upload**: The `channel_details` function uploads the gathered YouTube data to MongoDB.
- **SQL Integration**: The `channel_tables` function creates SQL tables and inserts data from MongoDB into PostgreSQL, ensuring no duplicates.

### Playlist Tables

The `playlist_tables` function creates a table named `playlists` in the PostgreSQL database and inserts playlist data from MongoDB into it.

```python
def playlist_tables(channel_name_s):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    create_query = '''
    CREATE TABLE IF NOT EXISTS playlists (
        Playlist_Id VARCHAR(255) PRIMARY KEY,
        Title VARCHAR(255),
        Channel_Id VARCHAR(255),
        Channel_Name VARCHAR(255),
        PublishedAt TIMESTAMP,
        Video_Count INT
    )
    '''
    
    cursor.execute(create_query)
    mydb.commit()

    solo_playlist_detail = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_info.Channel_Name": channel_name_s}, {"_id": 0}):
        solo_playlist_detail.append(ch_data["playlist_info"])
    df_solo_playlist_detail = pd.DataFrame(solo_playlist_detail[0])

    for index, row in df_solo_playlist_detail.iterrows():
        insert_query = '''
        INSERT INTO playlists (
            Playlist_Id,
            Title,
            Channel_Id,
            Channel_Name,
            PublishedAt,
            Video_Count
        ) VALUES (%s, %s, %s, %s, %s, %s)
        '''
        
        values = (
            row['Playlist_Id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            row['PublishedAt'],
            row['Video_Count']
        )
        
        cursor.execute(insert_query, values)
        mydb.commit()
```

###  Video Tables

The `video_tables` function creates a table named `videos` in the PostgreSQL database and inserts video data from MongoDB into it.

```python
def video_tables(channel_name_s):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    create_query = '''
    CREATE TABLE IF NOT EXISTS videos (
        Channel_Name VARCHAR(255),
        Channel_Id VARCHAR(255),
        Video_Id VARCHAR(255) PRIMARY KEY,
        Video_Name VARCHAR(255),
        Video_Description TEXT,
        Tags TEXT,
        PublishedAt TIMESTAMP,
        View_Count BIGINT,
        Like_Count BIGINT,
        Favorite_Count INT,
        Comment_Count INT,
        Duration INTERVAL,
        Thumbnail VARCHAR(255),
        Caption_Status VARCHAR(255)
    )
    '''
    
    cursor.execute(create_query)
    mydb.commit()

    solo_video_detail = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_info.Channel_Name": channel_name_s}, {"_id": 0}):
        solo_video_detail.append(ch_data["video_info"])
    df_solo_video_detail = pd.DataFrame(solo_video_detail[0])

    for index, row in df_solo_video_detail.iterrows():
        insert_query = '''
        INSERT INTO videos (
            Channel_Name,
            Channel_Id,
            Video_Id,
            Video_Name,
            Video_Description,
            Tags,
            PublishedAt,
            View_Count,
            Like_Count,
            Favorite_Count,
            Comment_Count,
            Duration,
            Thumbnail,
            Caption_Status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        
        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Video_Id'],
            row['Video_Name'],
            row['Video_Description'],
            row['Tags'],
            row['PublishedAt'],
            row['View_Count'],
            row['Like_Count'],
            row['Favorite_Count'],
            row['Comment_Count'],
            row['Duration'],
            row['Thumbnail'],
            row['Caption_Status']
        )
        
        cursor.execute(insert_query, values)
        mydb.commit()
```

### Summary

 **Playlist Tables**:
   - Creates a `playlists` table if it doesn't exist.
   - Retrieves playlist information for a specific channel from MongoDB.
   - Inserts playlist data into the PostgreSQL `playlists` table.

 **Video Tables**:
   - Creates a `videos` table if it doesn't exist.
   - Retrieves video information for a specific channel from MongoDB.
   - Inserts video data into the PostgreSQL `videos` table.

### Usage

To use these functions, simply call them with the `channel_name_s` parameter, which should be the name of the channel you want to process. Make sure to handle any potential exceptions or errors that might occur during database operations.

###  Comment Tables

The `comment_tables` function creates a table named `comments` in the PostgreSQL database and inserts comment data from MongoDB into it. The function first drops the table if it exists to ensure a clean slate.

```python
def comment_tables(channel_name_s):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        database="youtube_data",
        port="5432"
    )

    cursor = mydb.cursor()

    # Drop the table if it exists
    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create the comments table
    create_query = '''
    CREATE TABLE IF NOT EXISTS comments (
        Comment_Id VARCHAR(255) PRIMARY KEY,
        Comment_Text TEXT,
        Comment_Author VARCHAR(255),
        Comment_Published TIMESTAMP,
        Video_Id VARCHAR(255)
    )
    '''
    
    cursor.execute(create_query)
    mydb.commit()

    solo_comment_detail = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({"channel_info.Channel_Name": channel_name_s}, {"_id": 0}):
        solo_comment_detail.append(ch_data["comment_info"])
    df_solo_comment_detail = pd.DataFrame(solo_comment_detail[0])

    for index, row in df_solo_comment_detail.iterrows():
        insert_query = '''
        INSERT INTO comments (
            Comment_Id,
            Comment_Text,
            Comment_Author,
            Comment_Published,
            Video_Id
        ) VALUES (%s, %s, %s, %s, %s)
        '''   

        values = (
            row['Comment_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_Published'],
            row['Video_Id']
        )
        
        cursor.execute(insert_query, values)
        mydb.commit()
```

###  Create All Tables

The `tables` function orchestrates the creation and population of all tables (channels, playlists, videos, and comments) in the PostgreSQL database.

```python
def tables(solo_channel):
    meow = channel_tables(solo_channel)
    if meow:
        return meow
    else:
        playlist_tables(solo_channel)
        video_tables(solo_channel)
        comment_tables(solo_channel)

        return "Tables created successfully"
```

###  Show DataFrames

The following functions are used to display the data from MongoDB collections in Streamlit dataframes.

#### Show Channels Table

```python
def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for ch_data in collection1.find({}, {"_id": 0, "channel_info": 1}):
        ch_list.append(ch_data["channel_info"])
    df = st.dataframe(ch_list)

    return df
```

#### Show Playlists Table

```python
def show_playlists_table():
    pl_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for pl_data in collection1.find({}, {"_id": 0, "playlist_info": 1}):
        for i in range(len(pl_data['playlist_info'])):
            pl_list.append(pl_data['playlist_info'][i])
    df1 = st.dataframe(pl_list)

    return df1
```

#### Show Videos Table

```python
def show_videos_table():
    vi_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for vi_data in collection1.find({}, {"_id": 0, "video_info": 1}):
        for i in range(len(vi_data['video_info'])):
            vi_list.append(vi_data['video_info'][i])
    df2 = st.dataframe(vi_list)

    return df2
```

#### Show Comments Table

```python
def show_comments_table():
    comm_list = []
    db = client["Youtube_data"]
    collection1 = db["channel_details"]
    for comm_data in collection1.find({}, {"_id": 0, "comment_info": 1}):
        for i in range(len(comm_data['comment_info'])):
            comm_list.append(comm_data['comment_info'][i])
    df3 = st.dataframe(comm_list)

    return df3
```

### Summary

1. **Comment Tables**:
   - Drops the `comments` table if it exists.
   - Creates a new `comments` table.
   - Retrieves comment information for a specific channel from MongoDB.
   - Inserts comment data into the PostgreSQL `comments` table.

2. **Create All Tables**:
   - Calls functions to create and populate the channels, playlists, videos, and comments tables.

3. **Show DataFrames**:
   - Functions to display data from MongoDB collections in Streamlit dataframes.

These functions together provide a complete pipeline for extracting data from the YouTube API, storing it in MongoDB, transferring it to PostgreSQL, and displaying it using Streamlit.

This Streamlit script implements a user interface for collecting YouTube channel data, storing it in MongoDB, migrating the data to SQL, and querying the SQL database for insights.

```python
import streamlit as st
import pandas as pd
import psycopg2
from pymongo import MongoClient

# MongoDB client setup
client = MongoClient("mongodb+srv://rohanchristopher468:rohanchris@cluster0.dpmo0aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["Youtube_data"]

# Sidebar setup
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

# Input for channel ID
channel_id = st.text_input("Enter the channel ID")

# Button for collecting and storing data
if st.button("collect and store data"):
    ch_ids = []
    collection1 = db["channel_details"]
    for ch_data in collection1.find({}, {"_id": 0, "channel_info": 1}):
        ch_ids.append(ch_data["channel_info"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
        insert = channel_details(channel_id)  # Assuming channel_details function is defined
        st.success(insert)

# Dropdown to select a unique channel
all_channel = []
for ch_data in collection1.find({}, {"_id": 0, "channel_info": 1}):
    all_channel.append(ch_data["channel_info"]["Channel_Name"])
unique_channel = st.selectbox("select the Channel", all_channel)

# Button for migrating to SQL
if st.button("Migrate to SQL"):
    Table = tables(unique_channel)  # Assuming tables function is defined
    st.success(Table)

# Radio buttons to select the table to view
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

# Display the selected table
if show_table == "CHANNELS":
    show_channels_table()  # Assuming show_channels_table function is defined
elif show_table == "PLAYLISTS":
    show_playlists_table()  # Assuming show_playlists_table function is defined
elif show_table == "VIDEOS":
    show_videos_table()  # Assuming show_videos_table function is defined
elif show_table == "COMMENTS":
    show_comments_table()  # Assuming show_comments_table function is defined

# SQL connection
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="postgres",
    database="youtube_data",
    port="5432"
)
cursor = mydb.cursor()

# Dropdown to select a query
question = st.selectbox("Select Your question", (
    "1. names of all the videos and channel name",
    "2. channels have the most number of videos",
    "3. top 10 most viewed videos",
    "4. comments were made on each video",
    "5. videos have the highest number of likes",
    "6. total number of likes",
    "7. total number of views for each channel",
    "8. published videos in the year 2022",
    "9. average duration of all videos in each channel",
    "10 videos with highest number of comments"
))

# Execute and display the selected query
if question == "1. names of all the videos and channel name":
    query1 = '''SELECT video_name AS title, channel_name AS channelname FROM videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df1 = pd.DataFrame(t1, columns=["videos title", "channel name"])
    st.write(df1)

elif question == "2. channels have the most number of videos":
    query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos FROM channels ORDER BY total_videos DESC'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["channel name", "no of videos"])
    st.write(df2)

elif question == "3. top 10 most viewed videos":
    query3 = '''SELECT view_count AS views, channel_name AS channelname, video_name AS title FROM videos WHERE view_count IS NOT NULL ORDER BY views DESC LIMIT 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["views", "channel name", "Video title"])
    st.write(df3)

elif question == "4. comments were made on each video":
    query4 = '''SELECT comment_count AS comments, video_name AS title FROM videos WHERE comment_count IS NOT NULL'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["no of comments", "video title"])
    st.write(df4)

elif question == "5. videos have the highest number of likes":
    query5 = '''SELECT video_name AS title, channel_name AS channelname, like_count AS likes FROM videos WHERE like_count IS NOT NULL ORDER BY like_count DESC'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["video title", "channelname", "likes"])
    st.write(df5)

elif question == "6. total number of likes":
    query6 = '''SELECT like_count AS likes, video_name AS title FROM videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["likes", "video title"])
    st.write(df6)

elif question == "7. total number of views for each channel":
    query7 = '''SELECT channel_name AS channelname, channel_views AS totalviews FROM channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channel name", "total views"])
    st.write(df7)

elif question == "8. published videos in the year 2022":
    query8 = '''SELECT video_name AS title, publishedat AS videorelease, channel_name AS channelname FROM videos WHERE EXTRACT(YEAR FROM publishedat) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["video title", "video release", "channel name"])
    st.write(df8)

elif question == "9. average duration of all videos in each channel":
    query9 = '''SELECT channel_name AS channelname, AVG(duration) AS averageduration FROM videos GROUP BY channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channel name", "average duration"])
    t9_formatted = []
    for index, row in df9.iterrows():
        channel_title = row["channel name"]
        average_duration = row["average duration"]
        average_duration_str = str(average_duration)
        t9_formatted.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df1 = pd.DataFrame(t9_formatted)
    st.write(df1)

elif question == "10 videos with highest number of comments":
    query10 = '''SELECT video_name AS title, channel_name AS channelname, comment_count AS comments FROM videos WHERE comment_count IS NOT NULL ORDER BY comment_count DESC'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["video title", "channel name", "comments"])
    st.write(df10)
```

### Explanation:

 **Sidebar**:
   - Title, header, and captions to describe the project's skills and technologies.

 **Channel Data Collection**:
   - Text input to enter the channel ID.
   - Button to collect and store data. Checks if the channel data already exists in MongoDB.

 **Data Migration**:
   - Dropdown to select a unique channel.
   - Button to migrate the data to SQL.

 **Table Display**:
   - Radio buttons to select the table (channels, playlists, videos, comments) to view.
   - Display the selected table using Streamlit's dataframe display.

 **SQL Queries**:
   - Dropdown to select a query.
   - Execute the selected query and display the results in a dataframe.

This script provides an interactive way to manage and analyze YouTube data using Streamlit, MongoDB, and PostgreSQL. If you need further customization or additional features.
