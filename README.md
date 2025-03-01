# genkg

## Generic Recommendation Engine As a Service

### Steps to Deploy

1. **Package Glutton job** from the `glutton` directory. Refer to the [README.md](https://github.com/siddiqskm/genkg/tree/main/glutton) for details.
2. **Build the `genkg` and `glutton` services** using the following command:
   ```sh
   docker-compose build --no-cache
   ```
3. **Start the services** using:
   ```sh
   docker-compose up
   ```
4. **Create a Kafka topic** using:
   ```sh
   docker exec -it kafka1 kafka-topics --create --topic kg-ingestion --partitions 4 --replication-factor 2 --bootstrap-server kafka1:19092
   ```
   Output:
   ```
   Created topic kg-ingestion.
   ```
5. **Create a genkg database** by accessing OrientDB at:
   - [OrientDB Studio](http://localhost:2480/studio/index.html#/)
6. **Restart the `genkg` and `glutton-job` services** to start using the created topic and database.
7. **Access the FastAPI service docs** at:
   - [FastAPI Docs](http://localhost:8000/docs#/default/create_knowledge_graph_api_v1_kg_post)
8. **Create a directory named `test_data`** in the base directory and place the dataset here.
   - Movielens dataset can be downloaded from this link: [Movielens 32M](https://files.grouplens.org/datasets/movielens/ml-32m-README.html)
9. **Trigger knowledge graph creation** using the following JSON payload:
   ```json
   {
     "source": {
       "type": "local_files",
       "access_credentials": null,
       "path": "/opt/flink/test_data"
     },
     "vertices": [
       {
         "name": "movies",
         "file_pattern": "movies.csv",
         "columns": [
           {"name": "movieId", "type": "integer", "is_key": true},
           {"name": "title", "type": "string"},
           {"name": "genres", "type": "string"}
         ]
       },
       {
         "name": "ratings",
         "file_pattern": "ratings.csv",
         "columns": [
           {"name": "userId", "type": "integer"},
           {"name": "movieId", "type": "integer"},
           {"name": "rating", "type": "float"},
           {"name": "timestamp", "type": "long"}
         ]
       },
       {
         "name": "tags",
         "file_pattern": "tags.csv",
         "columns": [
           {"name": "userId", "type": "integer"},
           {"name": "movieId", "type": "integer"},
           {"name": "tag", "type": "string"},
           {"name": "timestamp", "type": "long"}
         ]
       },
       {
         "name": "links",
         "file_pattern": "links.csv",
         "columns": [
           {"name": "movieId", "type": "integer", "is_key": true},
           {"name": "imdbId", "type": "string"},
           {"name": "tmdbId", "type": "integer"}
         ]
       }
     ],
     "edges": [
       {
         "name": "user_rated_movie",
         "from_vertex": "ratings",
         "to_vertex": "movies",
         "file_pattern": "ratings.csv",
         "properties": [
           {"name": "rating", "type": "float"},
           {"name": "timestamp", "type": "long"}
         ],
         "mapping": {
           "from_key": "userId",
           "to_key": "movieId"
         }
       },
       {
         "name": "user_tagged_movie",
         "from_vertex": "tags",
         "to_vertex": "movies",
         "file_pattern": "tags.csv",
         "properties": [
           {"name": "tag", "type": "string"},
           {"name": "timestamp", "type": "long"}
         ],
         "mapping": {
           "from_key": "userId",
           "to_key": "movieId"
         }
       },
       {
         "name": "movie_has_link",
         "from_vertex": "movies",
         "to_vertex": "links",
         "file_pattern": "links.csv",
         "properties": [],
         "mapping": {
           "from_key": "movieId",
           "to_key": "movieId"
         }
       }
     ]
   }
   ```