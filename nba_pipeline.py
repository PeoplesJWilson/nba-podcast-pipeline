from airflow.decorators import dag,task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pendulum
import requests
import xmltodict
import pandas
import os
import json

feed_2 = "https://feeds.megaphone.fm/the-ringer-nba-show"
podcast_name_2 = "the ringer nba show"

feed_1 = "https://feeds.megaphone.fm/the-bill-simmons-podcast"
podcast_name_1 = "the bill simmons podcast"

@dag(
    dag_id="nba_pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023,6,26),
    catchup=False 
)
def nba_pipeline():

    create_db = SqliteOperator(
    task_id="create_table",
    sql="""
        CREATE TABLE IF NOT EXISTS metadata (
        link TEXT PRIMARY KEY,
        title TEXT,
        podcast TEXT,
        date INTEGER,
        description TEXT);
        """,
        sqlite_conn_id="podcast_metadata"
    )

    @task()
    def get_metadata(feed, podcast_name):
        data = requests.get(feed)

        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]

        month_to_num_dict = {'Jan': 1,'Feb': 2, 'Mar': 3, 
                     'Apr': 4, 'May': 5, 'Jun': 6, 
                     'Jul': 7, 'Aug': 8, 'Sep': 9, 
                     'Oct': 10, 'Nov': 11, 'Dec': 12}
        
        metadata = []
        for episode in episodes:
            link = episode["enclosure"]["@url"]
            title = episode["title"]

            date_data = episode["pubDate"].split(' ')
            day = int(date_data[1])
            month = int(month_to_num_dict[date_data[2]])
            year  = int(date_data[3])

            date = 100*100*year + 100*month+day

            description = episode["description"]

            metadata.append([link,title,podcast_name,date,description])

        return metadata
    

    metadata_1 = get_metadata.override(task_id="get_metadata_1")(feed = feed_1, podcast_name = podcast_name_1)
    metadata_2 = get_metadata.override(task_id="get_metadata_2")(feed = feed_2, podcast_name = podcast_name_2)

    create_db.set_downstream(metadata_1)
    create_db.set_downstream(metadata_2)


    @task()
    def populate_db(metadata):
        hook = SqliteHook(sqlite_conn_id="podcast_metadata")
        stored = hook.get_pandas_df("SELECT * FROM metadata;")

        not_stored = []

        for row in metadata:
            link = row[0]
            
            if link not in stored["link"].values:
                title = row[1]
                podcast_name = row[2]
                date = row[3]
                description = row[4]

                not_stored.append([link,title,podcast_name,date,description])

        hook.insert_rows(table="metadata", rows = not_stored, target_fields=["link","title","podcast","date","description"])

    populate_db.override(task_id="populate_db_1")(metadata_1)
    populate_db.override(task_id="populate_db_2")(metadata_2)


    @task()
    def download_episodes(metadata):

        podcast_name = metadata[0][2]
        podcast_name = podcast_name.replace(' ','_')
        podcast_path = os.path.join("episodes",podcast_name)
        if not os.path.exists(podcast_path):
            os.makedirs(podcast_path)
            print(f"created directory for podcast {podcast_name}")

        num_downloaded = 0
        download_queue = [(row[0],row[1],row[2],row[3]) for row in sorted(metadata, key = lambda row: row[3], reverse = True)]

        for link,title,podcast_name,date in download_queue:
            print(f"episode published on {date}")
            podcast_name = podcast_name.replace(' ','_')
            name = title.lower().replace(' ', '_')
            filename = f"{name}.mp3"
            episode_path = os.path.join("episodes", podcast_name, filename)

            if ( not os.path.exists(episode_path) ) and (num_downloaded < 2):
                num_downloaded += 1
                print(f"downloading {filename}") 
                audio = requests.get(link)
                with open(episode_path, "wb+") as f:
                    f.write(audio.content)

    
    download_episodes.override(task_id="download_episodes_1")(metadata_1)
    download_episodes.override(task_id="download_episodes_2")(metadata_2)


pipeline = nba_pipeline()

