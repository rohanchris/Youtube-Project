from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API key connection

def Api_connect():
    Api_Id="AIzaSyCp9UVxd1bw9-aIYUC2FDmW1AzjyMjpaPM"
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#get channel Info

def get_channel_info(channel_id):
    response=youtube.channels().list(
                                part="snippet,contentDetails,statistics",
                                id=channel_id).execute()

    for i in response["items"]:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscription_Count=i["statistics"]['subscriberCount'],
                Channel_Views=i['statistics']['viewCount'],
                Channel_Description=i['snippet']['description'],
                Total_videos=i['statistics']['videoCount'],
                Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
        
        return data
    
#get video ids

def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(
                                part='contentDetails',
                                id=channel_id).execute()

    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']   

    next_page_token=None

    while True:

        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

#get video info

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        response=youtube.videos().list(
                                    part='snippet,contentDetails,statistics',
                                    id=video_id).execute()
        
        for i in response['items']:
            data=dict(Channel_Name=i['snippet']['channelTitle'],
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
        
#get playlist info

def get_playlist_info(channel_id):
        next_page_token=None
        All_data=[]

        while True:
                response=youtube.playlists().list(
                                        part='snippet,contentDetails',
                                        channelId=channel_id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()

                for i in response['items']:
                        data=dict(Playlist_Id=i['id'],
                                Title=i['snippet']['title'],
                                Channel_Id=i['snippet']['channelId'],
                                Channel_Name=i['snippet']['channelTitle'],
                                PublishedAt=i['snippet']['publishedAt'],
                                Video_Count=i['contentDetails']['itemCount'])
                        All_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break

        return All_data

#get comment info

def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            response=youtube.commentThreads().list(
                                            part='snippet',
                                            videoId=video_id,
                                            maxResults=100).execute()
            for i in response['items']:
                data=dict(Comment_Id=i['snippet']['topLevelComment']['id'],
                        Comment_Text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=i['snippet']['topLevelComment']['snippet']['publishedAt'],
                        Video_Id=i['snippet']['topLevelComment']['snippet']['videoId'])
                
                comment_data.append(data)
                
    except:
        pass
    return comment_data


# upload to MongoDB

client=pymongo.MongoClient("mongodb+srv://rohanchristopher468:rohanchris@cluster0.dpmo0aw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    pl_details=get_playlist_info(channel_id)
    comm_details=get_comment_info(vi_ids)

    collection1=db["channel_details"]
    collection1.insert_one({"channel_info":ch_details,
                            "video_info":vi_details,
                            "playlist_info":pl_details,
                            "comment_info":comm_details})
    

    return "uploaded Successfully"

#SQL Table for Channels,Videos,Playlists,Comments

def channel_tables(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="youtube_data",
                        port="5432")

    cursor=mydb.cursor()


    create_query='''create table if not exists channels(Channel_Name varchar(255),
                                                        Channel_Id varchar(255) primary key,
                                                        Subscription_Count bigint,
                                                        Channel_Views bigint,
                                                        Channel_Description text,
                                                        Total_videos int,
                                                        Playlist_Id varchar(255))'''
    
    cursor.execute(create_query)
    mydb.commit()


    solo_channel_detail=[]
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({"channel_info.Channel_Name":channel_name_s},{"_id":0}):
        solo_channel_detail.append(ch_data["channel_info"])
    df_solo_channel_detail=pd.DataFrame(solo_channel_detail)

    for index,row in df_solo_channel_detail.iterrows():
        insert_query='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Channel_Views,
                                            Channel_Description,
                                            Total_videos,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['Total_videos'],
                row['Playlist_Id'])
        
        try:
              
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            meow=f"Your Provided Channel Name {channel_name_s} is already exists"

            return meow



def playlist_tables(channel_name_s):
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="youtube_data",
                        port="5432")

        cursor=mydb.cursor()

        create_query='''create table if not exists playlists(Playlist_Id varchar(255) primary key,
                                                        Title varchar(255),
                                                        Channel_Id varchar(255),
                                                        Channel_Name varchar(255),
                                                        PublishedAt timestamp,
                                                        Video_Count int)'''

        cursor.execute(create_query)
        mydb.commit()

        solo_playlist_detail=[]
        db=client["Youtube_data"]
        collection1=db["channel_details"]
        for ch_data in collection1.find({"channel_info.Channel_Name":channel_name_s},{"_id":0}):
            solo_playlist_detail.append(ch_data["playlist_info"])
        df_solo_playlist_detail=pd.DataFrame(solo_playlist_detail[0])

        for index,row in df_solo_playlist_detail.iterrows():
                insert_query='''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
        
                values=(row['Playlist_Id'],
                        row['Title'],
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['PublishedAt'],
                        row['Video_Count']
                        )
                
                
                cursor.execute(insert_query,values)
                mydb.commit()

def video_tables(channel_name_s):
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="youtube_data",
                        port="5432")

        cursor=mydb.cursor()

        create_query='''create table if not exists videos(Channel_Name varchar(255),
                                                        Channel_Id varchar(255),
                                                        Video_Id varchar(255) primary key,
                                                        Video_Name varchar(255),
                                                        Video_Description text,
                                                        Tags text,
                                                        PublishedAt timestamp,
                                                        View_Count bigint,
                                                        Like_Count bigint,
                                                        Favorite_Count int,
                                                        Comment_Count int,
                                                        Duration interval,
                                                        Thumbnail varchar(255),
                                                        Caption_Status varchar(255))'''

        cursor.execute(create_query)
        mydb.commit()

        solo_video_detail=[]
        db=client["Youtube_data"]
        collection1=db["channel_details"]
        for ch_data in collection1.find({"channel_info.Channel_Name":channel_name_s},{"_id":0}):
                solo_video_detail.append(ch_data["video_info"])
        df_solo_video_detail=pd.DataFrame(solo_video_detail[0])

        for index,row in df_solo_video_detail.iterrows():
                insert_query='''insert into videos(Channel_Name,
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
                                                        Caption_Status)
                                                
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                
                values=(row['Channel_Name'],
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
                
                
                cursor.execute(insert_query,values)
                mydb.commit()

def comment_tables(channel_name_s):
        mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="postgres",
                        database="youtube_data",
                        port="5432")

        cursor=mydb.cursor()

        drop_query='''drop table if exists comments'''
        cursor.execute(drop_query)
        mydb.commit()

        create_query='''create table if not exists comments(Comment_Id varchar(255) primary key,
                                                        Comment_Text text,
                                                        Comment_Author varchar(255),
                                                        Comment_Published timestamp,
                                                        Video_Id varchar(255))'''

        cursor.execute(create_query)
        mydb.commit()

        solo_comment_detail=[]
        db=client["Youtube_data"]
        collection1=db["channel_details"]
        for ch_data in collection1.find({"channel_info.Channel_Name":channel_name_s},{"_id":0}):
                 solo_comment_detail.append(ch_data["comment_info"])
        df_solo_comment_detail=pd.DataFrame(solo_comment_detail[0])


        for index,row in df_solo_comment_detail.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published,
                                                        Video_Id)
                                                
                                                        values(%s,%s,%s,%s,%s)'''   

                values=(row['Comment_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'],
                        row['Video_Id']
                        )
                        
                cursor.execute(insert_query,values)
                mydb.commit()


def tables(solo_channel):
    meow= channel_tables(solo_channel)
    if meow:
          return meow
    
    else:
        playlist_tables(solo_channel)
        video_tables(solo_channel)
        comment_tables(solo_channel)

        return "Tables created successfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_info":1}):
        ch_list.append(ch_data["channel_info"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for pl_data in collection1.find({},{"_id":0,"playlist_info":1}):
        for i in range(len(pl_data['playlist_info'])):
                pl_list.append(pl_data['playlist_info'][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for vi_data in collection1.find({},{"_id":0,"video_info":1}):
        for i in range(len(vi_data['video_info'])):
                vi_list.append(vi_data['video_info'][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    comm_list=[]
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for comm_data in collection1.find({},{"_id":0,"comment_info":1}):
        for i in range(len(comm_data['comment_info'])):
                comm_list.append(comm_data['comment_info'][i])
    df3=st.dataframe(comm_list)

    return df3


#streamlit part

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    collection1=db["channel_details"]
    for ch_data in collection1.find({},{"_id":0,"channel_info":1}):
        ch_ids.append(ch_data["channel_info"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

all_channel=[]
db=client["Youtube_data"]
collection1=db["channel_details"]
for ch_data in collection1.find({},{"_id":0,"channel_info":1}):
    all_channel.append(ch_data["channel_info"]["Channel_Name"])


unique_channel=st.selectbox("select the Channel",all_channel)

if st.button("Migrate to SQL"):
        Table=tables(unique_channel)
        st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
     show_channels_table()

elif show_table=="PLAYLISTS":
     show_playlists_table()

elif show_table=="VIDEOS":
     show_videos_table()

elif show_table=="COMMENTS":
     show_comments_table()


#SQL Connection

mydb=psycopg2.connect(host="localhost",
                user="postgres",
                password="postgres",
                database="youtube_data",
                port="5432")

cursor=mydb.cursor()

question=st.selectbox("Select Your question",("1. names of all the videos and channel name",
                                              "2. channels have the most number of videos",
                                              "3. top 10 most viewed videos",
                                              "4. comments were made on each video",
                                              "5. videos have the highest number of likes",
                                              "6. total number of likes",
                                              "7. total number of views for each channel",
                                              "8. published videos in the year 2022",
                                              "9.  average duration of all videos in each channel",
                                              "10 videos with highest number of comments"))


if question=="1. names of all the videos and channel name":
    query1='''select video_name as title,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["videos title","channel name"])
    df1
    st.write(df1)

elif question=="2. channels have the most number of videos":
        query2='''select channel_name as channelname,total_videos as no_videos from channels 
                    order by total_videos desc'''
        cursor.execute(query2)
        mydb.commit()
        t2=cursor.fetchall()
        df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
        df2
        st.write(df2)

elif question=="3. top 10 most viewed videos":
        query3='''select view_count as views,channel_name as channelname,video_name as title from videos
                    where view_count is not null order by views desc limit 10'''
        cursor.execute(query3)
        mydb.commit()
        t3=cursor.fetchall()
        df3=pd.DataFrame(t3,columns=["views","channel name","Video title"])
        df3
        st.write(df3)

elif question=="4. comments were made on each video":
        query4='''select comment_count as comments,video_name as title from videos where comment_count is not null'''
        cursor.execute(query4)
        mydb.commit()
        t4=cursor.fetchall()
        df4=pd.DataFrame(t4,columns=["no of comments","video title"])
        df4
        st.write(df4)

elif question=="5. videos have the highest number of likes":
        query5='''select video_name as title,channel_name as channelname,like_count as likes from videos
                where like_count is not null order by like_count desc'''
        cursor.execute(query5)
        mydb.commit()
        t5=cursor.fetchall()
        df5=pd.DataFrame(t5,columns=["video title","channelname","likes"])
        df5
        st.write(df5)

elif question=="6. total number of likes":
        query6='''select like_count as likes,video_name as title from videos'''
        cursor.execute(query6)
        mydb.commit()
        t6=cursor.fetchall()
        df6=pd.DataFrame(t6,columns=["likes","video title"])
        df6
        st.write(df6)

elif question=="7. total number of views for each channel":
        query7='''select channel_name as channelname,channel_views as totalviews from channels'''
        cursor.execute(query7)
        mydb.commit()
        t7=cursor.fetchall()
        df7=pd.DataFrame(t7,columns=["channel name","total views"])
        df7
        st.write(df7)

elif question=="8. published videos in the year 2022":
        query8='''select video_name as title,publishedat as videorelease,channel_name as channelname from videos 
                where extract(year from publishedat)=2022'''
        cursor.execute(query8)
        mydb.commit()
        t8=cursor.fetchall()
        df8=pd.DataFrame(t8,columns=["video title","video release","channel name"])
        df8
        st.write(df8)

elif question=="9.  average duration of all videos in each channel":
        query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group
                by channel_name'''
        cursor.execute(query9)
        mydb.commit()
        t9=cursor.fetchall()
        df9=pd.DataFrame(t9,columns=["channel name","average duration"])
        df9

        T9=[]
        for index,row in df9.iterrows():
                channel_title=row["channel name"]
                average_duration=row["average duration"]
                average_duration_str=str(average_duration)
                T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
        df1=pd.DataFrame(T9)
        df1
        st.write(df1)

elif question=="10 videos with highest number of comments":
        query10='''select video_name as title,channel_name as channelname,comment_count as comments from videos where comment_count
                is not null order by comment_count desc'''
        cursor.execute(query10)
        mydb.commit()
        t10=cursor.fetchall()
        df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
        df10
        st.write(df10)