import praw, os

reddit = praw.Reddit(
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    username=os.getenv('USERNAME'),
    password=os.getenv('PASSWORD'),
    user_agent=os.getenv('USER_AGENT')
)

OAuth2_Client_ID = os.getenv('OAUTH2_CLIENT_ID')
OAuth2_Client_Secret = os.getenv('OAUTH2_CLIENT_SECRET')

api_key = os.getenv('CONSUMER_KEY')
api_secret = os.getenv('CONSUMER_SECRET')
bearer_token = os.getenv('BEARER_TOKEN')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
