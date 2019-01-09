from minio import Minio
from minio.error import ResponseError
import re
import numpy
import json
import os

#Interface with minio (note: does not handle errors)
minioClient = Minio('127.0.0.1:9000',
                  access_key='AKIAIOSFODNN7EXAMPLE',
                  secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
                  secure=False)

def main():
    #get obj from minio
    txt_obj = minioClient.get_object('bucket1', 'sometext.txt')
    #Normalize data taken from minio
    rm_chars = '().;,'
    text = txt_obj.data.decode('utf-8').lower()
    for c in rm_chars: text = text.replace(c, ' ')
    word_list = re.split('\n|\r| ', text)
    word_list = list(filter(None, word_list))

    #Generates list with top 20 words
    unq_wordlist, indices, vals = numpy.unique(word_list, return_inverse=True, return_counts=True)
    top20_list = [{word: int(val)} for val, word in sorted(zip(vals, unq_wordlist), reverse=True)][0:20]

    #Generate stats from the data
    stats_dict = {
        'count' : int(len(word_list)),
        'unique': int(len(unq_wordlist)),
        'min' : {
            'words' : [],
            'val' : int(min(vals))
        },
        'max' : {
            'words': [],
            'val' : int(max(vals))
        },
        'mean' : str(numpy.mean(vals)),
        'std' : str(numpy.std(vals))
    }

    min_indices = [i for i, e in enumerate(vals) if e == min(vals)]
    max_indices = [i for i, e in enumerate(vals) if e == max(vals)]

    stats_dict['min']['words'] = [unq_wordlist[i] for i in min_indices]
    stats_dict['max']['words'] = [unq_wordlist[i] for i in max_indices]

    #Generate top 20 pairs of words
    wordpairs = []
    wordpair_dict = {}

    for word in top20_list:
        word_indices = [i for i, e in enumerate(word_list) if e == list(word)[0]]
        for index in word_indices:
            if index > 0 and word_list[index - 1] != '':
                wordpairs.append((word_list[index - 1], word_list[index]))
            if index < len(word_list) - 1 and word_list[index + 1] != '':
                wordpairs.append((word_list[index], word_list[index + 1]))
            word_list[index] = ''

    for pair in wordpairs:
        pair_key = ','.join(pair)
        if pair_key in wordpair_dict:
            wordpair_dict[pair_key] += 1
        else:
            wordpair_dict[pair_key] = 1

    sorted_pairs = sorted(wordpair_dict.items(), key=lambda kv: kv[1], reverse=True)
    top_20_wordpairs = [{x : y} for x, y in sorted_pairs][0:20]

    #outputs to file
    output_dict = {
        'top_20_words' : top20_list,
        'top_20_wordpairs' : top_20_wordpairs,
        'stats': stats_dict
    }

    with open('obj_storage/output.json', 'w') as outfile:
        json.dump(output_dict, outfile)

    #sends output file to minio
    with open('obj_storage/output.json', 'rb') as file_data:
        file_stat = os.stat('obj_storage/output.json')
        minioClient.put_object('bucket1', 'output.json', file_data, file_stat.st_size, content_type='application/json')

#determines if program needs to run
inp_obj = minioClient.stat_object('bucket1', 'sometext.txt')

try:
    out_obj = minioClient.stat_object('bucket1', 'output.json')
    if inp_obj.last_modified > out_obj.last_modified:
        print("input file has been updated or modified and output file has been updated")
        main()
    else:
        print("no modifications to output file")
except ResponseError.NoSuchKey as err:
    print("input file has been updated or modified and output file has been updated")
    main()
