from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json 

## Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date = days_ago(1),
    schedule_interval = '@daily',
    catchup=False,
) as dag :
    ## step 1: Create the table if it doesnt exist
    @task
    def create_table():
        ## initialize the Postgres Hook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        ## SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS nasa_apod (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );



        """
        # Execute the table creation query
        postgres_hook.run(create_table_query)

    ## step 2: Extract the NASA API Data(APOD)-Astronomy Picture of the Day[Extract pipeline]
    extract_apod = SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', # Connection ID defined in Airflow for NASA API
        endpoint='planetary/apod', # NASA API endpoint for APOD
        method='GET',
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},  # Using API Key from the connection
        response_filter = lambda response: response.json(),
    )

    ## step 3: Transform the data (Pick the information that i need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data
    
    
    ## step 4: Loading the data into the Postgres SQL 
    @task
    def load_data_to_postgres(apod_data):
        ## Initialize the Postgres Hook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        ## Define the SQL Insert Query
        insert_query = """
        INSERT INTO nasa_apod (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        ## Execute the SQL Query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'], 
            apod_data['url'], 
            apod_data['date'], 
            apod_data['media_type']
        ))

    ## step 5: Verify the data with DBViewer  



    ## Step 6: Define the task dependencies
    create_table() >> extract_apod 
    api_response = extract_apod.output
    ## Transform
    transformed_data = transform_apod_data(api_response)
    ##Load
    load_data_to_postgres(transformed_data)