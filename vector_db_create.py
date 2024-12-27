import utils
import weaviate.classes as wvc

client = utils.connect_to_db()

client.collections.delete("Movie")

reviews = client.collections.create(
    name="Review",
    vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_openai(),
    generative_config=wvc.config.Configure.Generative.openai(),

    properties=[
        wvc.config.Property(
            name="body",
            data_type=wvc.config.DataType.TEXT,
        )
    ]
)

movies = client.collections.create(
    name="Movie",
    vectorizer_config=wvc.config.Configure.Vectorizer.text2vec_openai(),
    generative_config=wvc.config.Configure.Generative.openai(),
    #properties are similar to columns in a table
    properties=[
        wvc.config.Property(
            name="title",
            data_type=wvc.config.DataType.TEXT,
        ),
        wvc.config.Property(
            name="description",
            data_type=wvc.config.DataType.TEXT,
        ),
        wvc.config.Property(
            name="movie_id",
            data_type=wvc.config.DataType.INT,
        ),
        wvc.config.Property(
            name="year",
            data_type=wvc.config.DataType.TEXT,
        ),
        wvc.config.Property(
            name="rating",
            data_type=wvc.config.DataType.NUMBER,
        ),
        wvc.config.Property(
            name="director",
            data_type=wvc.config.DataType.TEXT,
            skip_vectorization=True,
        ),
    ],

    references=[
        wvc.config.ReferenceProperty(
            name="hasReview",
            target_collection=reviews.name,
        )
    ]
)




client.close()