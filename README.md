# nba-podcast-pipeline
This README contains instructions on how to setup a data pipeline using airflow which does the following:
- scrapes podcast data using an RSS feed.
- saves podcast metadata to an sqlite database.
- saves podcast audio to local directory.
  
This project is a piece of a larger project which attempts to use nba podcaster sentiment, along with nba stats, to predict the outcome of games. This larger project is coming soon.
## Instructions
- Create a root directory for this project, and follow the instructions found [here](https://towardsdatascience.com/an-introduction-to-apache-airflow-21111bf98c1f) to install airflow within a virtual environment
- Navigate to the airflow folder resulting from the previous step, run the linux command "mkdir dags" to create a folder which will contain the pipeline
- Navigate to "dags" and clone this repo. Making sure you're in the virtual environment created in step 2, run "pip install -r requirements.txt"
- Create an empty directory "episodes" within dags. This will store mp3 files downloaded by the pipeline
- The pipeline is now ready to be run. Simply run "airflow scheduler" and "airflow webserver" (in two separate terminals)
- To view the pipeline in the airflow gui, go to "localhost" in your browser, and find "nba_pipeline"

  For airflow commands in the above steps to work, make sure you run "export AIRFLOW_HOME=/path/to/project-root/airflow"
