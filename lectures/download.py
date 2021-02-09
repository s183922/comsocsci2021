### GET COMMENTS ###
from tqdm import tqdm
from psaw import PushshiftAPI
import pandas as pd
from datetime import datetime
import numpy as np
df = pd.read_csv("reddit.csv")
api = PushshiftAPI()

sub_id = list(df["id"][df["num_comments"]> 0])
N = 400
steps = np.linspace(0, len(sub_id)-1, N, dtype = np.int)

date1 = int(datetime(2020,1,1).timestamp())
date2 = int(datetime(2021,1,25).timestamp())
subreddit = 'WallStreetBets'
query = "GME|Gamestop"

comments = []
for i in tqdm(range(1,N)):
    ids = sub_id[steps[i-1]:steps[i]]
    gen_comments = api.search_comments(subreddit = subreddit,
                                      after = date1,
                                      before = date2,
                                      link_id = ids)
    comments.extend(list(gen_comments))
    
data = [(p.d_["id"],
         p.d_["link_id"],
         p.d_["score"],
         p.d_["created_utc"],
         p.d_["author"],
         p.d_["parent_id"]) for p in comments]
columns = ["id", "submission_id", "score", "creation_date", "author", "parent_id"]
df_comments = pd.DataFrame(data, columns = columns)
df_comments.to_csv("comments.csv", index = False)