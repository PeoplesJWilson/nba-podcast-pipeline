from airflow.decorators import dag,task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pendulum
import requests
import xmltodict
import pandas
import os
import json 
from pydub import AudioSegment

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
        CREATE TABLE IF NOT EXISTS episodes (
        link TEXT PRIMARY KEY,
        title TEXT,
        filename TEXT,
        date INTEGER,
        description TEXT);
        """,
        sqlite_conn_id="podcasts"
    )

    @task()
    def get_episode_metadata():
        data = requests.get("https://feeds.megaphone.fm/the-ringer-nba-show")
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        return episodes
    

    episodes = get_episode_metadata()
    create_db.set_downstream(episodes)


    @task()
    def populate_db(episodes):
        hook = SqliteHook(sqlite_conn_id="podcasts")
        stored = hook.get_pandas_df("SELECT * FROM episodes;")

        not_stored = []

        month_to_num_dict = {'Jan': 1,'Feb': 2, 'Mar': 3, 
                     'Apr': 4, 'May': 5, 'Jun': 6, 
                     'Jul': 7, 'Aug': 8, 'Sep': 9, 
                     'Oct': 10, 'Nov': 11, 'Dec': 12}
        for episode in episodes:
            link = episode["enclosure"]["@url"]
            title = episode["title"]

            name = episode["title"].lower().replace(' ', '_')
            filename = f"{name}.mp3"

            date_data = episode["pubDate"].split(' ')
            day = int(date_data[1])
            month = int(month_to_num_dict[date_data[2]])
            year  = int(date_data[3])
            date = 100*100*year + 100*month+day

            description = episode["description"]
            
            if link not in stored["link"].values:
                not_stored.append([link,title,filename,date,description])

        hook.insert_rows(table="episodes", rows = not_stored, target_fields=["link","title","filename","date","description"])

    populate_db(episodes)


    @task()
    def download_episodes(episodes):
        month_to_num_dict = {'Jan': 1,'Feb': 2, 'Mar': 3, 
                     'Apr': 4, 'May': 5, 'Jun': 6, 
                     'Jul': 7, 'Aug': 8, 'Sep': 9, 
                     'Oct': 10, 'Nov': 11, 'Dec': 12}
        
        for episode in episodes:

            name = episode["title"].lower().replace(' ', '_')
            filename = f"{name}.mp3"
            episode_path = os.path.join("episodes", filename)

            converted_filename = f"{name}.txt"
            converted_path = os.path.join("episodes",converted_filename)

            date_data = episode["pubDate"].split(' ')
            day = int(date_data[1])
            month = int(month_to_num_dict[date_data[2]])
            year  = int(date_data[3])
            date = 100*100*year + 100*month+day

            if ( not os.path.exists(episode_path) ) and ( not os.path.exists(converted_path) ) and ( date > 100*100*2023 + 100*6 ):
                print(f"downloading {filename}") 
                audio = requests.get(episode["enclosure"]["@url"])
                with open(episode_path, "wb+") as f:
                    f.write(audio.content)

    
    download_episodes(episodes)


pipeline = nba_pipeline()