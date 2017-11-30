#!/usr/bin/python3
import argparse
import csv
import json
'''
Compare csv

'''

parser=argparse.ArgumentParser()
parser.add_argument("-src", "--source-input", dest="src",
                    help="required: csv file to compare", required=True)
parser.add_argument("-dest", "--dest-input", dest="dest",
                    help="required: csv file to compare with", required=True)
parser.add_argument("-d", "--delimiter", dest="delimiter",
                    help="csv delimiter - default - , ", default=',')
parser.add_argument("-keys", "--compare-keys", dest="keys",
                    help="compare by this key or combination of more than one key seperated by comma", required=True)
parser.add_argument("--encoding", dest="encoding",
                    help="use this encoding to open the files", default='UTF-8')


args = vars(parser.parse_args())

src = args['src']
dest = args['dest']
delimiter = args['delimiter']
keys=args['keys'].split(",")
encoding=args['encoding']

print("keys: %s"%(keys))



def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    modified = {o : (d1[o], d2[o]) for o in intersect_keys if d1[o] != d2[o]}
    return modified

def csv_to_dict_by_keys(input_file,keys,delimiter):
    d=dict()
    with open(input_file, encoding=encoding )as f:
        csv_h = csv.DictReader(f,delimiter=delimiter)
        for line in csv_h:
            id_list = [line[k] for k in keys]
            id="-".join(id_list)
            if id in d:
                raise KeyError('key conflict in file %s, try to add existing key, line number %s and line %s\nKeys: %s'%(input_file,csv_h.line_num,  d[id]['line_number'],str(keys)))
                continue
            d[id]=dict(data=line,line_number=csv_h.line_num)
    return d


def process_results(results):
    '''
    :param results:
    :return: amount of changes per field.
    '''
    d=dict()
    for res in results:
        for field in res[1]:
            try:
                d[field] +=1
            except KeyError:
                d[field] =1
    return d


def results_precentage(procceced_results,number_of_lines):
    '''
    :param procceced_results: process_results function output
    :param number_of_lines: base number to calculate avarage
    :return: statistics in percentage
    '''
    d=dict()
    for field_name,sum_per_field in procceced_results.items():
        d[field_name] = (sum_per_field*100)/number_of_lines
    return d


def write_to_csv(dict,filename,delimiter=delimiter):
    with open(filename,'w') as f:
        writer = csv.writer(f,delimiter=delimiter)
        for key, value in dict.items():
            writer.writerow([key, value])

def write_json_results(results,filename):
    with open(filename,"w") as f:
        f.write(json.dumps(results,sort_keys=True,indent=4))




src_d = csv_to_dict_by_keys(src,keys,delimiter)
dest_d = csv_to_dict_by_keys(dest,keys,delimiter)

results = []
lines_modified = 0
for key in src_d:
    src_dict  = src_d[key]['data']
    try:
        dest_dict = dest_d[key]['data']
    except KeyError:
        lines_modified += 1
        results.append((src_d[key]['line_number'],{'No Field':'Line not exist on dest csv'}))
        continue
    modified = dict_compare(src_dict,dest_dict)
    if len(modified) > 0:
        lines_modified += 1
        results.append((src_d[key]['line_number'],modified))


field_change_sum = process_results(results)

write_to_csv(field_change_sum,'field_change_sum.csv')
write_to_csv(results_precentage(field_change_sum,len(src_d)),'field_change_percent.csv')
write_json_results(results,'results_details.json')




