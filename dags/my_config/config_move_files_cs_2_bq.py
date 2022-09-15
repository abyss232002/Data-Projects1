import google.auth

# Get PROJECT_ID
_, PROJECT_ID = google.auth.default()

# Set env based on project id
if PROJECT_ID == "river-treat-362102":
    env = "dev"
elif PROJECT_ID == "river-treat-462102":
    env = "uat"
elif PROJECT_ID == "river-treat-562102":
    env = "prod"


source_bucket = f'{env}_rich_karma_staging'
source_folder = 'data'
file_pattern = 'us-500'
bq_project = 'river-treat-362102'
bq_database = 'staging'
bq_table = 'us_500'
bq_table_disp = 'WRITE_TRUNCATE'
order_schema = "Schema/schema_orders.json"
