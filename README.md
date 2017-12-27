# helper scripts

here you can find the following scripts:
1. `csv_diff.py` compare two csv files<br>
1.  `elasticsearch2csv.py` export data from elasticsearche to csv <br>




##cvs diff

The script creates two files:<br> 
`field_change_sum.csv` - contain the number of changes in each field.<br>
`results_details.json` - contain the changes in JSON format.


```bash
usage: csv_diff.py [-h] -src SRC -dest DEST [-d DELIMITER] -keys KEYS
                   [--encoding ENCODING]

optional arguments:
  -h, --help            show this help message and exit
  -src SRC, --source-input SRC
                        required: csv file to compare
  -dest DEST, --dest-input DEST
                        required: csv file to compare with
  -d DELIMITER, --delimiter DELIMITER
                        csv delimiter - default - ,
  -keys KEYS, --compare-keys KEYS
                        compare by this key or combination of more than one
                        key seperated by comma
  --encoding ENCODING   use this encoding to open the files

```




##elasticsearch to csv
 Tool for exporting elasticsearch query to CSV file assumption: the response document is not multidimensional(nested)
 document. it will execute elasticsearch query_string, for example query_string: this AND that OR thus.
 it can handle large number of Elasticsearch results (even all results) by using `Scroll`.<br>
example for usage:<br>
`python3 elasticsearch2csv.py -i index_name -t type_name  --query_string "this AND that"`
```bash
usage: elasticsearch2csv.py [-h] -i INDEX -t TYPE [--host HOST]
                            [--output OUTPUT] [-d DELIMITER]
                            [--query JSON_QUERY] [--query_string QUERY_STRING]
                            [--fields FIELDS] [--size SIZE]

optional arguments:
  -h, --help            show this help message and exit
  -i INDEX, --index INDEX
                        required: index name
  -t TYPE, --type TYPE  required: doc type name
  --host HOST           host url default=localhost
  --output OUTPUT       output file default: output.csv
  -d DELIMITER, --delimiter DELIMITER
                        csv delimiter - default - ,
  --query JSON_QUERY    json customized query if query exists, query_string
                        will be ignored
  --query_string QUERY_STRING
                        query string 'this AND that OR thus'
  --fields FIELDS       comma separated fields - default all
  --size SIZE           size of the respons, default 1000

```
