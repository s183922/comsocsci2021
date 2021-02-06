import pandas as pd
from threading import Thread
import numpy as np

df = pd.read_csv("reddit.csv")


def download_comments(ids, data):
    
    for id in ids:
        gen =  api.search_comments(link_id = str(id))
        results = list(gen)
        data.extend([(p.d_["id"], str(id), p.d_["score"], p.d_["created_utc"], p.d_["author"], p.d_["parent_id"]) for p in results])
    return data

ids = df["id"][df["num_comments"] > 0]

threads = 5
steps = np.linspace(0,len(ids), threads, dtype=np.int)
jobs = []
data = []


for i in range(1, threads):
    
    thread = threading.Thread(target=download_comments(ids[steps[i-1]:steps[i]], data))
    jobs.append(thread)
    prev_step = step
    
for j in jobs:
    j.start()
    
for j in jobs:
    j.join()

columns = ["id", "submission_id", "score", "creation_date", "author", "parent_id"]
df_columns = pd.DataFrame(data, columns = columns)
df_columns.to_csv("comments.csv", index = False)