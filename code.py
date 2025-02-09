import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

@dlt.resource(name="rides", write_disposition="replace")
def taxi_data():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    )
    paginator = PageNumberPaginator()
    yield from client.paginate("/", paginator=paginator)

pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

# Run the pipeline
load_info = pipeline.run(taxi_data())
print(load_info)

# Check created tables
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
print(conn.sql("DESCRIBE").df())

# Get record count
rides = pipeline.dataset(dataset_type="default").rides
print(f"Total records: {len(list(rides))}")

# Calculate average trip duration
with pipeline.sql_client() as client:
    res = client.execute_sql("""
        SELECT AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
        FROM rides;
    """)
    print(f"Average trip duration: {res[0][0]} minutes")