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


source_bucket = f'{env}_rich_karma_landing'
source_folder = 'data'
target_bucket = f'{env}_rich_karma_staging'
target_folder = 'data'
file_pattern = 'us-500'
