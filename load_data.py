import utils
import weaviate.classes as wvc
from weaviate.util import generate_uuid5
import pandas as pd

client = utils.connect_to_db()

movies = client.collections.get("Movie")
reviews = client.collections.get("Review")

movie_df = pd.read_csv("data/movies.csv")

review_objs = list()
for i, row in movie_df.iterrows():
    for c in [1, 2, 3]:
        col_name = f"Critic Review {c}"
        if len(row[col_name]) > 0:
            props = {
                "body" : row[col_name],
            }
            review_uuid = generate_uuid5(row[col_name])
            data_obj = wvc.data.DataObject(
                properties=props,
                uuid=review_uuid,
            )
            review_objs.append(data_obj)

response = reviews.data.insert_many(review_objs)

print(f"Insertion complete with {len(response.all_responses)} objects")
print(f"Insertions errors: {len(response.errors)}")

movie_objs = list()
for index, row in movie_df.iterrows():
    props = {
        "title" : row["Movie Title"],
        "description" : row["Description"],
        "movie_id" : row["ID"],
        "year" : row["Year"],
        "rating" : row["Star Rating"],
        "director" : row["Director"],
    }

    review_uuids = list()
    for c in [1, 2, 3]:
        col_name = f"Critic Review {c}"
        if len(row[col_name]) > 0:
            review_uuid = generate_uuid5(row[col_name])
            review_uuids.append(review_uuid)
    
    movie_uuid = generate_uuid5(row["ID"])
    data_obj = wvc.data.DataObject(
        properties=props,
        uuid=movie_uuid,
        references={"hasReview" : review_uuids},
    )
    movie_objs.append(data_obj)

response = movies.data.insert_many(movie_objs)

print(f"Insertion complete with {len(response.all_responses)} objects")
print(f"Insertions errors: {len(response.errors)}")

client.close()
