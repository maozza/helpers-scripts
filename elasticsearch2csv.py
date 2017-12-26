#!/usr/bin/python3
import elasticsearch
import argparse
import csv
from elasticsearch import helpers
import json

'''
Tool for exporting elasticsearch query to CSV file
assumption: the response document is not multidimensional(nested) document.
it will execute elasticsearch query_string,
for example query_string: this AND that OR thus
'''

parser=argparse.ArgumentParser()
parser.add_argument("-i", "--index", dest="index",
                    help="required: index name", required=True)
parser.add_argument("-t", "--type", dest="type",
                    help="required: doc type name", required=True)
parser.add_argument("--host", dest="host",
                    help="host url default=localhost", default='localhost')
parser.add_argument("--output", dest="output",
                    help="output file default: output.csv", default="output.csv")
parser.add_argument("-d", "--delimiter", dest="delimiter",
                    help="csv delimiter - default - , ", default=',')
parser.add_argument("--query", dest="json_query",
                    help="json customized query if query exists, query_string will be ignored", default=None)

parser.add_argument("--query_string", dest="query_string",
                    help="query string \'this AND that OR thus\'", default="*")
parser.add_argument("--fields", dest="fields",
                    help="comma separated fields - default all", default='all')
parser.add_argument("--size", dest="size",
                    help="size of the respons, default 1000", default='1000')
args = vars(parser.parse_args())

host = args['host']
index = args['index']
doc_type = args['type']
output_files = args['output']
query_string = args['query_string']
delimiter = args['delimiter']
fields= args['fields']
size=int(args['size'])
json_query = args['json_query']



'''
Create elasticsearch instance
'''
es = elasticsearch.Elasticsearch(host)


'''
Get query from input
'''
if json_query:
    query = json.loads(json_query)
else:
    query = dict(
        query = dict(
            query_string=dict(
                query=query_string
            )
        )
    )


'''
Get real index name in case index is alias
'''
aliases = es.indices.get_alias()
for i in aliases:
    if index in aliases[i]['aliases']:
        index = i



'''
Fetch the mapping in order to create the header
'''
mapping=es.indices.get_mapping(index=index,doc_type=doc_type)[index]['mappings'][doc_type]['properties'].keys()

'''
Set handler to elasticsearch
'''
scanResp= helpers.scan(client=es, query=query, scroll= "10m", index=index,size=size, doc_type=doc_type, clear_scroll=False, request_timeout=300)



with open(output_files, 'w') as f:
    counter = 0
    if fields == "all":
        w = csv.DictWriter(f, mapping, delimiter=delimiter,quoting=csv.QUOTE_MINIMAL)
    else:
        fields = fields.split(",")
        w = csv.DictWriter(f, [i for i in mapping if i in fields], delimiter=delimiter, extrasaction='ignore',quoting=csv.QUOTE_MINIMAL )
    w.writeheader()
    try:
        for row in scanResp:
            _ = w.writerow(row['_source'])
            counter +=1
    except elasticsearch.exceptions.NotFoundError:
        pass
    except elasticsearch.exceptions.RequestError:
        pass

if counter > 0:
    print('%s lines was expotred to file: %s'%(counter,output_files))
else:
    if len(host.split(':')) == 1:
        host +=':9200'
    print('no data to export from source:\n'
          '%s\nGET /%s/%s/_search'%(host,index,doc_type))
    print(json.dumps(query,indent=4, sort_keys=True))
