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
    

IN_DATASET = 'preparation'
IN_TABLE = 'prep_us_500'
OUT_BUCKET = f'{env}_rich_karma_presentation'
OUT_FOLDER = 'data'
OUT_FILE_NAME = 'prep_us_500.csv'
