import os
import sys
import json
import zipfile
import pandas as pd

# Get filename
file_arg = sys.argv[1]
dis = file_arg.split("/")[-1][:-4]
user_col = ["id","creation_timestamp","description","favourites_count","followers_count",
            "friends_count", "geo_tag","banner_link","display_image_link","status_count",
            "verified_check", "anchor_tweet", "anchor_tweet_date","disorder"]
tweet_col = ["disorder_flag", "text", "conversation_id", "tweet_id", "language", 
             "likes_count", "quote_count", "reply_count", "retweet_count", "source_name",
             "timestamp_tweet", "mentionedUsers", "media", "user_id", "covid"]

# Database retrieve
if os.path.exists(f'output/{dis}/users_{dis}.csv'):
    users_data = pd.read_csv(f'output/{dis}/users_{dis}.csv', dtype=str, lineterminator=";")
    users = users_data[users_data.disorder == dis]['id'].values
else:
    users_data = pd.DataFrame(columns=user_col)
    users = []
print("Numbers of transformed users:", len(users))

# Filter directories (only the one which haven't processed)
covid_date = '2020-03-11'
archive = zipfile.ZipFile(f'{file_arg}', 'r')
listdirs = archive.namelist()
dirs = []
for s in listdirs: 
    s_split = s.split("/")
    if len(s_split) == int(sys.argv[2]) and s.endswith("/") and s_split[-2] \
       not in users: dirs.append(s)
print("Total users left:", len(dirs))

# Function to save file, concat data only at the end
def save_file(users, tweets):
    users_data = pd.concat(users)
    tweets_data = pd.concat(tweets)
    print(len(tweets_data['user_id'][tweets_data['user_id'].isnull()]))

    if os.path.exists(f'output/{dis}/users_{dis}.csv'):
        users_data.to_csv(f'output/{dis}/users_{dis}.csv', mode='a', header=False, 
                          index=False, lineterminator=";")
        tweets_data.to_csv(f'output/{dis}/tweets_{dis}.csv', mode='a', header=False, 
                           index=False, lineterminator=";")
    else:
        users_data.to_csv(f'output/{dis}/users_{dis}.csv', index=False, lineterminator=";")
        tweets_data.to_csv(f'output/{dis}/tweets_{dis}.csv', index=False, lineterminator=";")


# Run transformation
total_user = 0
while total_user != len(dirs):
    i = 0
    users_new = []
    tweets_new = []
    try:
        for dir in dirs[total_user:]:
            namespace = dir.split("/")
            user_id = namespace[-2]
            i += 1
            total_user += 1
            print("=", end="", flush=True)

            if dis == 'neg':
                anchor = {}
            else:
                anchor = json.load(archive.open(dir + 'anchor_tweet.json'))

            user = json.load(archive.open(dir + 'user.json'))
            tweets = json.load(archive.open(dir + 'tweets.json'))

            user_data = {**user, **anchor}
            user_df = pd.DataFrame(user_data, index=[0])
            user_df['disorder'] = namespace[5]

            tweet_temp = []
            for keydate, tweet in tweets.items():
                tweet_df = pd.DataFrame(tweet)
                tweet_df['user_id'] = user_id
                tweet_df['covid'] = 1 if keydate > covid_date else 0
                tweet_df['media'] = tweet_df['media'] \
                    .apply(lambda x: str([m["type"] for m in x]))
                tweet_df = tweet_df.reindex(columns=tweet_col).fillna("")
                tweet_temp.append(tweet_df)

            user_df = user_df.reindex(columns=user_col).fillna("")
            users_new.append(user_df)
            tweets_new += tweet_temp

            if i == 100:
                print()
                print("Taking a break...")
                break

        save_file(users_new, tweets_new)
        print("Let's do it again!", total_user, "out of", len(dirs))

    # Enabling intermittent transformation with keyboard interruption
    except KeyboardInterrupt:
        print("Interrupt execution...")
        save_file(users_new, tweets_new)
        break
