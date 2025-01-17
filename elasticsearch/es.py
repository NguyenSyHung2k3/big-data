from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

query = {
    "query": {
        "term": {
            "Market": "VN"
        }
    },
    "aggs": {
        "genre_counts": {
            "terms": {
                "field": "Genre",
                "size": 10  
            },
            "aggs": {
                "total_count": {
                    "sum": {
                        "field": "Count"
                    }
                }
            }
        }
    },
    "size": 0 
}

response = es.search(index="cdef", body=query)

for bucket in response['aggregations']['genre_counts']['buckets']:
    print(f"Genre: {bucket['key']}, Total Count: {bucket['total_count']['value']}")
