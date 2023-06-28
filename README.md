from datetime import datetime
from datetime import datetime as dt

import mysql.connector
import pandas as pd
import pymongo
import streamlit as st
from googleapiclient.discovery import build
from sqlalchemy.engine import URL, create_engine
from sqlalchemy import text


def convert_to_sql_datetime(timestamp):
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    sql_datetime = dt.strftime("%Y-%m-%d %H:%M:%S")
    return sql_datetime


# Function to format duration
def convert_duration(duration):
    duration = duration[2:]  # Remove 'PT' from the duration string
    time_parts = {"H": 0, "M": 0, "S": 0}

    # Extract the hours, minutes, and seconds from the duration string
    for part in time_parts:
        if part in duration:
            time_parts[part], duration = duration.split(part)
            time_parts[part] = int(time_parts[part])

    # Format the duration as HH:MM:SS
    formatted_duration = "{:02d}:{:02d}:{:02d}".format(
        time_parts["H"], time_parts["M"], time_parts["S"]
    )

    return formatted_duration


def FormattedDurationToSeconds(string):
    list_str = string.split(":")
    hh = int(list_str[0])
    mm = int(list_str[1])
    ss = int(list_str[2])
    return hh * 3600 + mm * 60 + ss


# Retrieve YouTube information
def CreateYTdict(CHANNEL_ID):
    channels_response = (
        youtube.channels()
        .list(part="status,snippet,statistics,contentDetails", id=CHANNEL_ID)
        .execute()
    )

    chan_sections_response = (
        youtube.channelSections()
        .list(part="snippet,contentDetails", channelId=CHANNEL_ID)
        .execute()
    )

    # Channel_Name dictionary creation:
    ch_id = channels_response["items"][0].get("id", "")

    if "snippet" in channels_response["items"][0]:
        ch_name = channels_response["items"][0]["snippet"].get("title", "")
        ch_desc = channels_response["items"][0]["snippet"].get("description", 0)
    else:
        ch_name = ""
        ch_desc = ""

    if "statistics" in channels_response["items"][0]:
        sub_count = int(channels_response["items"][0]["statistics"].get("subscriberCount", 0))
        views = int(channels_response["items"][0]["statistics"].get("viewCount", 0))
    else:
        sub_count = 0
        views = 0

    if "contentDetails" in channels_response["items"][0]:
        ch_playlist = channels_response["items"][0]["contentDetails"]["relatedPlaylists"].get(
            "uploads"
        )
    else:
        ch_playlist = ""

    if "status" in channels_response["items"][0]:
        status = channels_response["items"][0]["status"].get("privacyStatus", "")
    else:
        status = ""

    if "snippet" in chan_sections_response["items"]:
        ch_type = chan_sections_response["items"][0]["snippet"].get("type", "")
    else:
        ch_type = ""

    Channel_Name = {
        "channel_name": ch_name,
        "channel_id": ch_id,
        "channel_type": ch_type,
        "subscription_count": sub_count,
        "channel_views": views,
        "channel_description": ch_desc,
        "playlist_id": ch_playlist,
        "channel_status": status,
    }

    yt_pl_id = Channel_Name["playlist_id"]

    play_res = youtube.playlists().list(part="snippet", id=yt_pl_id).execute()

    play_title = play_res["items"][0]["snippet"].get("title", "")

    playlist = {
        "playlist": {"playlist_id": yt_pl_id, "channel_id": ch_id, "playlist_title": play_title}
    }

    videos = []
    next_page_token = None
    while True:
        playlist_response = (
            youtube.playlistItems()
            .list(part="snippet", playlistId=yt_pl_id, maxResults=50, pageToken=next_page_token)
            .execute()
        )

        videos += playlist_response["items"]
        next_page_token = playlist_response.get("nextPageToken")
        if next_page_token is None:
            break

    video_list = []
    for video in videos:
        vid = video["snippet"]["resourceId"]["videoId"]

        try:
            video_response = (
                youtube.videos().list(id=vid, part="snippet,statistics,contentDetails").execute()
            )
            if "snippet" in video_response["items"][0]:
                vname = video_response["items"][0]["snippet"].get("title", "")
                vdesc = video_response["items"][0]["snippet"].get("description", "")
                vtags = video_response["items"][0]["snippet"].get("tags", [])
                vpubat = video_response["items"][0]["snippet"].get("publishedAt", "")
            else:
                vname = ""
                vdesc = ""
                vtags = ""
                vpubat = ""

            if "statistics" in video_response["items"][0]:
                view_count = int(video_response["items"][0]["statistics"].get("viewCount", 0))
                like_count = int(video_response["items"][0]["statistics"].get("likeCount", 0))
                dislike_count = int(video_response["items"][0]["statistics"].get("dislikeCount", 0))
                fav_count = int(video_response["items"][0]["statistics"].get("favoriteCount", 0))
                comment_count = int(video_response["items"][0]["statistics"].get("commentCount", 0))
            else:
                view_count = 0
                like_count = 0
                dislike_count = 0
                fav_count = 0
                comment_count = 0

            if "contentDetails" in video_response["items"][0]:
                duration = video_response["items"][0]["contentDetails"].get("duration", "")
                caption_status = video_response["items"][0]["contentDetails"].get("caption", "")
            else:
                duration = ""
                caption_status = ""

            if "default" in video["snippet"]["thumbnails"]:
                thumbnail = video["snippet"]["thumbnails"]["default"].get("url", "")
            else:
                thumbnail = ""

            video_dict = {
                "video_id": vid,
                "playlist_id": yt_pl_id,
                "video_name": vname,
                "video_description": vdesc,
                "video_tags": vtags,
                "PublishedAt": vpubat,
                "View_count": view_count,
                "like_count": like_count,
                "dislike_count": dislike_count,
                "favourite_count": fav_count,
                "comment_count": comment_count,
                "duration": convert_duration(duration),
                "thumbnail": thumbnail,
                "caption_status": caption_status,
            }

        except Exception as e:
            print(e)

        comments = []
        next_page_token = None
        flag = True
        while flag:
            try:
                comment_response = (
                    youtube.commentThreads()
                    .list(
                        part="snippet,replies",
                        videoId=vid,
                        maxResults=10,
                        pageToken=next_page_token,
                    )
                    .execute()
                )
                comments.append(comment_response["items"])
                next_page_token = comment_response.get("nextPageToken")
                if next_page_token is None:
                    flag = False
            except Exception as e:
                print(e)
                comments.append([])
                flag = False

        # Comments Dictionary
        if len(comments[0]) >= 1:
            comments_dict = {}
            for i in range(len(comments[0])):
                comment = comments[0][i]
                comments_dict[f"Comment_Id_{i+1}"] = {
                    "comment_id": comment["id"],
                    "video_id": vid,
                    "comment_text": comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    "comment_author": comment["snippet"]["topLevelComment"]["snippet"][
                        "authorDisplayName"
                    ],
                    "comment_publishedAt": comment["snippet"]["topLevelComment"]["snippet"][
                        "publishedAt"
                    ],
                }
        else:
            comments_dict = {}

        video_dict.update({"comments": comments_dict})
        video_list.append(video_dict)

    videos_dictionary = {}
    for i in range(len(video_list)):
        video = video_list[i]
        videos_dictionary[f"Video_Id_{i+1}"] = video

    final_dict = {}
    final_dict["Channel"] = Channel_Name
    final_dict.update(playlist)
    final_dict.update(videos_dictionary)

    return final_dict


def get_db_collection():
    uri = "mongodb+srv://kpavan8121:NhWHlMhYccbuwOIt@cluster0.hyxwhaf.mongodb.net/?retryWrites=true&w=majority"
    dbname = "YTharvDB"
    collection_name = "channel"
    client = pymongo.MongoClient(uri)
    db = client[dbname]
    collection = db[collection_name]
    return collection


def Insert_data_to_MongoDB_collection(data):
    yt_collection = get_db_collection()
    yt_collection.insert_one(data)


def get_data_info_in_MongoDB():
    yt_collection = get_db_collection()
    yt_data = yt_collection.find()
    docs = []
    for item in yt_data:
        docs.append(item)
    channels_list = []
    try:
        for item in docs:
            ch_name = item["Channel"]["channel_name"]
            channels_list.append(ch_name)
    except Exception as e:
        print(e)
    return channels_list


def connect_to_mysql():
    connection = mysql.connector.connect(
        host="127.0.0.1", port=3306, user="root", password="@lexandeR@8121", database="youtube"
    )
    return connection


def migrate_channel_to_mysql(channel_id):
    yt_collection = get_db_collection()
    connection = connect_to_mysql()
    cursor = connection.cursor()

    # Retrieve data from MongoDB collection
    data = yt_collection.find()

    docs = []
    for item in data:
        docs.append(item)

    for i, item in enumerate(docs):
        if item["Channel"]["channel_id"] == channel_id:
            get_index = i

    item = docs[get_index]

    # Create Channel table in MySQL
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Channel (
            channel_id VARCHAR(255),
            channel_name VARCHAR(255),
            channel_type VARCHAR(255),
            channel_views INT,
            channel_subscribers INT,
            channel_description TEXT,
            channel_status VARCHAR(255)
        )
    """
    )

    # Insert channel data into MySQL table
    channel_id = item["Channel"]["channel_id"]
    channel_name = item["Channel"]["channel_name"]
    channel_type = item["Channel"]["channel_type"]
    channel_views = item["Channel"]["channel_views"]
    cahnnel_subscribers = item["Channel"]["subscription_count"]
    channel_description = item["Channel"]["channel_description"]
    channel_status = item["Channel"]["channel_status"]

    sql = """
        INSERT INTO Channel
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        channel_id,
        channel_name,
        channel_type,
        channel_views,
        cahnnel_subscribers,
        channel_description,
        channel_status,
    )
    cursor.execute(sql, values)

    connection.commit()
    cursor.close()
    connection.close()


def migrate_playlist_to_mysql(channel_id):
    yt_collection = get_db_collection()
    connection = connect_to_mysql()
    cursor = connection.cursor()

    # Retrieve data from MongoDB collection
    data = yt_collection.find()

    docs = []
    for item in data:
        docs.append(item)

    for i, item in enumerate(docs):
        if item["Channel"]["channel_id"] == channel_id:
            get_index = i

    item = docs[get_index]

    # Create Playlist table in MySQL
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Playlist (
            playlist_id VARCHAR(255),
            channel_id VARCHAR(255),
            playlist_name VARCHAR(255)
        )
    """
    )

    # Insert playlist data into MySQL table

    playlist_id = item["playlist"]["playlist_id"]
    channel_id = item["Channel"]["channel_id"]
    playlist_name = item["playlist"]["playlist_title"]

    sql = """
        INSERT INTO Playlist
        VALUES (%s, %s, %s)
    """
    values = (playlist_id, channel_id, playlist_name)
    cursor.execute(sql, values)

    connection.commit()
    cursor.close()
    connection.close()


def migrate_videos_to_mysql(channel_id):
    yt_collection = get_db_collection()
    connection = connect_to_mysql()
    cursor = connection.cursor()

    # Retrieve data from MongoDB collection
    data = yt_collection.find()

    docs = []
    for item in data:
        docs.append(item)

    for i, item in enumerate(docs):
        if item["Channel"]["channel_id"] == channel_id:
            get_index = i

    item = docs[get_index]

    # Create Video table in MySQL
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Videos (
            video_id VARCHAR(255),
            playlist_id VARCHAR(255),
            video_name VARCHAR(255),
            video_description TEXT,
            published_date DATETIME,
            view_count INT,
            like_count INT,
            dislike_count INT,
            favourite_count INT,
            comment_count INT,
            duration INT,
            thumbnail VARCHAR(255),
            caption_status VARCHAR(255)
        )
    """
    )

    # Insert video data into MySQL table
    video_values = []
    itm_keys = list(item.keys())
    for itm in itm_keys:
        if itm.startswith("Video"):
            del item[itm]["video_tags"]
            del item[itm]["comments"]
            dur_str = item[itm]["duration"]
            mod_dur_in_secs = FormattedDurationToSeconds(dur_str)
            item[itm]["duration"] = mod_dur_in_secs
            pubat = item[itm]["PublishedAt"]
            mod_pubat = convert_to_sql_datetime(pubat)
            item[itm]["PublishedAt"] = mod_pubat
            video_values.append(tuple(item[itm].values()))

    sql = """
        INSERT INTO Videos
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    cursor.executemany(sql, video_values)

    connection.commit()
    cursor.close()
    connection.close()


def migrate_comments_to_mysql(channel_id):
    yt_collection = get_db_collection()
    connection = connect_to_mysql()
    cursor = connection.cursor()

    # Retrieve data from MongoDB collection
    data = yt_collection.find()

    docs = []
    for item in data:
        docs.append(item)

    for i, item in enumerate(docs):
        if item["Channel"]["channel_id"] == channel_id:
            get_index = i

    item = docs[get_index]

    # Create Comment table in MySQL
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS Comments (
            comment_id VARCHAR(255),
            video_id VARCHAR(255),
            comment_text TEXT,
            comment_author VARCHAR(255),
            comment_published_date DATETIME
        )
    """
    )

    # Insert Comment data into MySQL table
    comment_values = []

    itm_keys = list(item.keys())
    for itm in itm_keys:
        if itm.startswith("Video") and len(item[itm]["comments"]) > 0:
            comments = item[itm]["comments"]

            for comment in comments.values():
                pubat = comment["comment_publishedAt"]
                mod_pubat = convert_to_sql_datetime(pubat)
                comment["comment_publishedAt"] = mod_pubat
                comment_values.append(tuple(comment.values()))

    sql = """
        INSERT INTO Comments
        VALUES (%s, %s, %s, %s, %s)
    """

    cursor.executemany(sql, comment_values)

    connection.commit()
    cursor.close()
    connection.close()


def migrate_data_to_mysql(channel_id):
    migrate_channel_to_mysql(channel_id)
    migrate_playlist_to_mysql(channel_id)
    migrate_videos_to_mysql(channel_id)
    migrate_comments_to_mysql(channel_id)


def MySql_connect():
    url_object = URL.create(
        drivername="mysql+mysqlconnector",
        username="root",
        password="@lexandeR@8121",
        host="127.0.0.1",
        port=3306,
        database="youtube",
    )
    engine = create_engine(url_object)

    return engine


def execute_sql_query(query):
    engine = MySql_connect()
    connection = engine.connect()
    statement = text(query)
    result = connection.execute(statement)
    rows = result.fetchall()
    connection.close()
    return rows


def get_sql_channels_info():
    query = """
        SELECT DISTINCT channel_name
        FROM channel;
    """
    df = pd.DataFrame(execute_sql_query(query), columns=["channel_name"])
    channels_list = list(df.channel_name.unique())
    return channels_list


def drop_existing_sql_tables():
    query = """
        DROP TABLE channel, playlist, videos, comments;
    """
    engine = MySql_connect()
    connection = engine.connect()
    connection.execute(query)
    connection.close()


def main():
    st.markdown("<h1 class='title'>Youtube Data Harvesting Project</h1>", unsafe_allow_html=True)

    if "myurls" not in st.session_state:
        st.session_state.myurls = {}

    if "urlclk" not in st.session_state:
        st.session_state.urlclk = []

    if "urlarr" not in st.session_state:
        st.session_state.urlarr = []

    if "rerun" not in st.session_state:
        st.session_state.rerun = False

    def listids():
        selected_ids = st.multiselect(
            "Select Channels to store their data in MongoDB",
            options=st.session_state.myurls,
            default=[],
        )
        return selected_ids

    if st.session_state.rerun:
        st.session_state.rerun = False
        st.experimental_rerun()
    else:
        channel_id = st.text_input(
            "Enter YouTube Channel ID", value="", placeholder="YouTube Channel ID"
        )
        if st.button("Add Channel"):
            if channel_id != "":
                try:
                    yt_dict = CreateYTdict(channel_id)
                    channel_name = yt_dict["Channel"]["channel_name"]
                    st.session_state.myurls.update([(channel_name, channel_id)])
                    st.session_state.urlarr.append(False)
                except:
                    st.write("Invalid Channel ID. Please type again!")

    selected_channels = listids()
    if st.button("Submit"):
        channel_dict = st.session_state.myurls
        mongo_channels = get_data_info_in_MongoDB()
        for channel in selected_channels:
            if channel in mongo_channels:
                print("Already Exists in MongoDB")
            else:
                mongo_channels.append(channel)
                channel_id = channel_dict[channel]
                yt_dict = CreateYTdict(channel_id)
                Insert_data_to_MongoDB_collection(yt_dict)

    if st.button("Migrate data to MySQL"):
        try:
            drop_existing_sql_tables()
        except:
            print("database empty")

        channel_dict = st.session_state.myurls

        for channel in selected_channels:
            channel_id = channel_dict[channel]
            migrate_data_to_mysql(channel_id)

        st.markdown("<h3>Video names and corresponding channels</h3>", unsafe_allow_html=True)
        query1 = """
            SELECT v.video_name, c.channel_id, c.channel_name
            FROM videos v
            INNER JOIN playlist p ON v.playlist_id = p.playlist_id
            INNER JOIN channel c ON p.channel_id = c.channel_id;
        """
        df1 = pd.DataFrame(
            execute_sql_query(query1), columns=["video_name", "channel_id", "channel_name"]
        )
        st.dataframe(df1)

        st.markdown(
            "<h3>Channels sorted by descending order of number of videos</h3>",
            unsafe_allow_html=True,
        )
        query2 = """
            SELECT c.channel_id, c.channel_name, count(v.video_id) AS video_count
            FROM videos v
            INNER JOIN playlist p ON v.playlist_id = p.playlist_id
            INNER JOIN channel c ON p.channel_id = c.channel_id
            GROUP BY c.channel_id, c.channel_name
            ORDER BY video_count DESC;
        """
        df2 = pd.DataFrame(
            execute_sql_query(query2), columns=["channel_id", "channel_name", "video_count"]
        )
        st.dataframe(df2)

        st.markdown("<h3>Top 10 most viewed videos</h3>", unsafe_allow_html=True)
        query3 = """
            select v.video_name, c.channel_name, v.view_count
            FROM videos v
            INNER JOIN playlist p ON v.playlist_id = p.playlist_id
            INNER JOIN channel c ON p.channel_id = c.channel_id
            ORDER BY v.view_count DESC
            LIMIT 10;
        """
        df3 = pd.DataFrame(
            execute_sql_query(query3), columns=["video_name", "channel_name", "view_count"]
        )
        st.dataframe(df3)

        st.markdown("<h3>Videos and corresponding number of comments</h3>", unsafe_allow_html=True)
        query4 = """
            SELECT video_id, video_name, comment_count
            FROM videos;
        """
        df4 = pd.DataFrame(
            execute_sql_query(query4), columns=["video_id", "video_name", "comment_count"]
        )
        st.dataframe(df4)

        st.markdown("<h3>Videos having highest likes</h3>", unsafe_allow_html=True)
        query5 = """
            SELECT v.video_id, v.video_name, c.channel_name, v.like_count
            FROM videos v
            INNER JOIN playlist p ON v.playlist_id = p.playlist_id
            INNER JOIN channel c ON p.channel_id = c.channel_id
            ORDER BY v.like_count DESC;
        """
        df5 = pd.DataFrame(
            execute_sql_query(query5),
            columns=["video_id", "video_name", "channel_name", "like_count"],
        )
        st.dataframe(df5)

        st.markdown("<h3>Number of likes and dislikes for each video</h3>", unsafe_allow_html=True)
        query6 = """
            SELECT video_id, video_name, like_count, dislike_count
            FROM videos;
        """
        df6 = pd.DataFrame(
            execute_sql_query(query6),
            columns=["video_id", "video_name", "like_count", "dislike_count"],
        )
        st.dataframe(df6)

        st.markdown("<h3>Number of views for each channel</h3>", unsafe_allow_html=True)
        query7 = """
            SELECT channel_name, channel_views
            FROM channel;
        """
        df7 = pd.DataFrame(execute_sql_query(query7), columns=["channel_name", "channel_views"])
        st.dataframe(df7)

        st.markdown("<h3>Channels that published a video in 2022</h3>", unsafe_allow_html=True)
        query8 = """
            SELECT DISTINCT c.channel_name
            FROM channel c
            INNER JOIN playlist p ON p.channel_id = c.channel_id
            INNER JOIN videos v ON v.playlist_id = p.playlist_id
            WHERE v.published_date LIKE  "2022-%";
        """
        df8 = pd.DataFrame(execute_sql_query(query8), columns=["channel_name"])
        st.dataframe(df8)

        st.markdown(
            "<h3>Average duration of all videos in each channel</h3>", unsafe_allow_html=True
        )
        query9 = """
            SELECT c.channel_id, c.channel_name, AVG(v.duration) AS avg_duration
            FROM channel c
            INNER JOIN playlist p ON p.channel_id = c.channel_id
            INNER JOIN videos v ON v.playlist_id = p.playlist_id
            GROUP BY c.channel_id, c.channel_name;
        """
        df9 = pd.DataFrame(
            execute_sql_query(query9), columns=["channel_id", "channel_name", "avg_duration"]
        )
        st.dataframe(df9)

        st.markdown("<h3>Videos having highest number of comments</h3>", unsafe_allow_html=True)
        query10 = """
            SELECT v.video_id, v.video_name, c.channel_name, v.comment_count
            FROM channel c
            INNER JOIN playlist p ON p.channel_id = c.channel_id
            INNER JOIN videos v ON v.playlist_id = p.playlist_id
            ORDER BY v.comment_count DESC;
        """
        df10 = pd.DataFrame(
            execute_sql_query(query10),
            columns=["video_id", "video_name", "channel_name", "comment_count"],
        )
        st.dataframe(df10)


if __name__ == "__main__":
    API_KEY = "AIzaSyD50RezO2qGl0Z7mYmlUcAH5EFIsMW-8q8"
    youtube = build("youtube", "v3", developerKey=API_KEY)
    main()

