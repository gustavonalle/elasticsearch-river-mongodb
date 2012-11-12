This is a forked MongoDB River Plugin for ElasticSearch, adding the possibility of specifying field mappings between mongodb documents
and elasticsearch ones.

Example, with the following mapping:

	curl -XPUT 'http://localhost:9200/_river/mongodb/_meta' -d '{
		"type": "mongodb",
		"mongodb": {
			"db": "testmongo",
			"collection": "person"
		},
		"index": {
			"name": "mongoindex",
			"type": "person"
		},
		"field_mappings": [{
			"generate": "FullName",
			"from": ["f","l"],
			"keep_source": true
		},
		{
			"generate": "age",
			"from": "a",
			"keep_source": false
		}]
	}'


A new (generated) field called "FullName" would be present in the Elastic Search document, and it'd composed of both "f" and "l" fields,
as an array containing all values. Since "keep_source" is true for this mapping, the original field "f" and "l" would also be present.

Also, a new field called "age" would be present in elastic search with the value of "a", and the the "a" field would not be present.


For a document in mongodb document like this:

	PRIMARY> db.person.findOne()
	{
		"_id" : 1,
		"f" : "David",
		"l" : "Cameron",
		"a" : 45
	}

The Elastic Search equivalent document would be:

	_source: {
    		FullName: [
        		"David",
        		"Cameron",
    		],
    		f: "David",
    		l: "Cameron",
    		age: 45
	}


Following is the original documentation of the plugin:


MongoDB River Plugin for ElasticSearch

    ---------------------------------------------------------
    | MongoDB River Plugin     | ElasticSearch    | MongoDB |
    --------------------------------------------------------|
    | master                   | 0.19.11-> master | 2.2.1   |
    --------------------------------------------------------|
    | 1.5.0                    | 0.19.11          | 2.2.1   |
    --------------------------------------------------------|
    | 1.4.0                    | 0.19.8           | 2.0.5   |
    --------------------------------------------------------|
    | 1.3.0                    | 0.19.4           |         |
    --------------------------------------------------------|
    | 1.2.0                    | 0.19.0           |         |
    --------------------------------------------------------|
    | 1.1.0                    | 0.19.0           | 2.0.2   |
    --------------------------------------------------------|
    | 1.0.0                    | 0.18             |         |
    --------------------------------------------------------|

Initial implementation by [aparo](https://github.com/aparo)

Modified to get the same structure as the others Elasticsearch river (like [CouchDB](http://www.elasticsearch.org/blog/2010/09/28/the_river_searchable_couchdb.html))

The latest version monitor oplog capped collection and support attachment (GridFS).

For the initial implementation see [tutorial](http://www.matt-reid.co.uk/blog_post.php?id=68#&slider1=4)


	curl -XPUT 'http://localhost:9200/_river/mongodb/_meta' -d '{
		"type": "mongodb", 
		"mongodb": { 
			"db": "DATABASE_NAME", 
			"collection": "COLLECTION", 
			"gridfs": true
		}, 
		"index": { 
			"name": "ES_INDEX_NAME", 
			"type": "ES_TYPE_NAME" 
		}
	}'

Example:

	curl -XPUT 'http://localhost:9200/_river/mongodb/_meta' -d '{ 
		"type": "mongodb", 
		"mongodb": { 
			"db": "testmongo", 
			"collection": "person"
		}, 
		"index": {
			"name": "mongoindex", 
			"type": "person" 
		}
	}'

Import data from mongo console:

	use testmongo
	var p = {firstName: "John", lastName: "Doe"}
	db.person.save(p)

Query index:

	curl -XGET 'http://localhost:9200/testmongo/_search?q=firstName:John'

	curl -XPUT 'http://localhost:9200/_river/mongodb/_meta' -d '{ 
		"type": "mongodb", 
		"mongodb": { 
			"db": "testmongo", 
			"collection": "files", 
			"gridfs": true 
		}, 
		"index": {
			"name": "mongoindex", 
			"type": "files" 
		}
	}'

Import binary content in mongo:

	%MONGO_HOME%\bin>mongofiles.exe --host localhost:27017 --db testmongo --collection files put test-document-2.pdf
	connected to: localhost:27017
	added file: { _id: ObjectId('4f230588a7da6e94984d88a1'), filename: "test-document-2.pdf", chunkSize: 262144, uploadDate: new Date(1327695240206), md5: "c2f251205576566826f86cd969158f24", length: 173293 }
	done!

Query index:

	curl -XGET 'http://localhost:9200/files/4f230588a7da6e94984d88a1?pretty=true'

See more details check the [wiki](https://github.com/richardwilly98/elasticsearch-river-mongodb/wiki)
