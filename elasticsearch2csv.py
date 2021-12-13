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
parser.add_argument("-t", "--type", dest="type", default='_doc',
                    help="required: doc type name")
parser.add_argument("--host", dest="host",
                    help="host url default=localhost", default='localhost')
parser.add_argument("--output", dest="output",
                    help="output file default: output", default="output")
parser.add_argument("-d", "--delimiter", dest="delimiter",
                    help="csv delimiter - default - , ", default=',')
parser.add_argument("--query", dest="json_query",
                    help="json customized query if query exists, query_string will be ignored, will also parse query from Kibana inspec", default=None)
parser.add_argument("--parse_query", dest="parse_query",
                    help="parse query from Kibana inspect", default=None)
parser.add_argument("--parse_query_file", dest="parse_query_file",
                    help="parse query from file, Kibana inspect", default=None)
parser.add_argument("--query_string", dest="query_string",
                    help="query string \'this AND that OR thus\'", default="*")
parser.add_argument("--fields", dest="fields",
                    help="comma separated fields - default all", default='all')
parser.add_argument("--size", dest="size",
                    help="size of the respons, default 1000", default='1000')
parser.add_argument("-u","--user", dest="user",
                    help="<user:password> Server user and password", default=None)
parser.add_argument("-f","--format", dest="format",
                    help="<csv|json> output file format", default="json")                    
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
parse_query = args['parse_query']
parse_query_file = args['parse_query_file']
auth = args['user']
format = args['format']

'''
Create elasticsearch instance
'''

if auth:
    if ":" in auth:
        user,password = auth.split(":",1)
    else:
        user = auth
        password = input("Enter host password for user '{}': ".format(user))
    es = elasticsearch.Elasticsearch(host,http_auth=(user, password))
else:
    es = elasticsearch.Elasticsearch(host)


'''
Get query from input
'''
if parse_query_file:
    with open(parse_query_file) as f:
       parse_query = f.read()

if json_query:
    query = json.loads(json_query)    
    if "query" in query:
        query = dict(query =query['query'] )
elif parse_query:
    query = json.loads(parse_query)
    query = dict(query =query['query'] )   
else:
    query = dict(
        query = dict(
            query_string=dict(
                query=query_string
            )
        )
    )

query['size'] = size

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
mapping=es.indices.get_mapping(index=index,doc_type=doc_type,include_type_name=True)[index]['mappings'][doc_type]['properties'].keys()

'''
Set handler to elasticsearch
'''
scanResp= helpers.scan(client=es, query=query, scroll="10m", index=index,size=size, doc_type=doc_type, clear_scroll=True, request_timeout=300)


if format == "csv":
    if not output_files.endswith(".csv"):
        output_files += ".csv" 
    with open(output_files, 'w') as f:
        counter = 0
        if fields == "all":
            w = csv.DictWriter(f, mapping, delimiter=delimiter,extrasaction='ignore',quoting=csv.QUOTE_MINIMAL)
            w.writeheader()
        else:
            fields = fields.split(",")
            w = csv.DictWriter(f, [i for i in mapping if i in fields], delimiter=delimiter, extrasaction='ignore',quoting=csv.QUOTE_MINIMAL )
            w.fieldnames = fields
            w.writeheader()
        try:
            for row in scanResp:
                if counter >= size:
                    break;
                _ = w.writerow(row['_source'])
                counter +=1
        except elasticsearch.exceptions.NotFoundError:
            pass
        except elasticsearch.exceptions.RequestError:
            pass

if format == "json":
    if not output_files.endswith(".json"):
        output_files += ".json"     
    with open(output_files, 'w') as f:
        counter = 0
        try:
            for row in scanResp:
                if counter >= size:
                    break;
                _ = f.write(json.dumps(row['_source'])+"\n")
                counter +=1
        except elasticsearch.exceptions.NotFoundError:
            pass
        except elasticsearch.exceptions.RequestError:
            pass            

if counter > 0:
    print('%s lines was exportred to file: %s'%(counter,output_files))
else:
    if len(host.split(':')) == 1:
        host +=':9200'
    print('no data to export from source:\n'
          '%s\nGET /%s/%s/_search'%(host,index,doc_type))
    print(json.dumps(query,indent=4, sort_keys=True))
