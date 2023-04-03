import json

from elasticsearch import Elasticsearch, helpers

client = Elasticsearch('http://localhost:9200')


def filterKeys(document, use_keys):
    use_keys.remove('id')
    return {key: document[key] for key in use_keys}


def doc_generator(df):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
                "_index": 'fias',
                "_id" : f"{document['id']}",
                "_source": filterKeys(document, df.columns.tolist()),
            }


def upload(df):
    create_index()
    helpers.bulk(client, doc_generator(df))


def create_index():
    body = json.load(open('fias2es/fias_mapping.json'))
    client.options(ignore_status=400).indices.create(
        index='fias',
        settings=body['settings'],
        mappings=body['mappings'],
    )
