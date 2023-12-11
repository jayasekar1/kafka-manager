'''
Read Source Topics from file
Read Feature Topics from file
Compare the Changes
Output changes to List of Actions File .
Admin to Review and approve Target topic file .
Execution Script takes over , reads Target topic file and executes the necessary rest api calls .
'''

from github import Github
from deepdiff import DeepDiff
from datetime import datetime

import re
import click
import json
import logging
import os
import string
import subprocess
import re
import requests
feature_file_path = 'C:\\Users\\venka\\Desktop\\KafkaAutomation\\feature_topics.json'
source_file_path = 'C:\\Users\\venka\\Desktop\\KafkaAutomation\\source_topics.json'

def main():

    # Read Source File..................................................
    source_file = open(source_file_path)
    read_topics = source_file.read()
    source_topics_list = json.loads(read_topics)
    source_topics_dict = {}
    for i in source_topics_list:
        source_topics_dict.update(i)
    source_file.close()

    # Read Feature file ..................................................
    feature_file = open(feature_file_path)
    read_topics = feature_file.read()
    feature_topics_list = json.loads(read_topics)
    feature_topics_dict = {}
    for i in feature_topics_list:
        feature_topics_dict.update(i)
    feature_file.close()

    # Compare Source & Feature Dictionaries ...............................
    st = source_topics_dict
    ft = feature_topics_dict
    diff = DeepDiff(st,ft,ignore_order=True)
    print(diff)
    list_of_actions=[]
    x = 0
    for i,j in diff.items():
        print('\n'+str(i))
        if (i == 'dictionary_item_added'):
            temp = re.findall(r"\['(.*?)'\]", str(j))
            e =temp[0]
            listentry = 'Add_Topic' + ';' + str(e)
            list_of_actions.append(listentry)

        if (i == 'dictionary_item_removed'):
            temp = re.findall(r"\['(.*?)'\]", str(j))
            e = temp[0]
            listentry = 'Delete_Topic' + ';' + str(e)
            list_of_actions.append(listentry)

        if (i == 'values_changed'):
            print(i)
            for x,y in j.items():
                temp = re.findall(r"\['(.*?)'\]", str(x))
                if "partitions_count" in temp:
                    listentry = 'update_topic'+';'+temp[0]+';'+temp[1]
                    list_of_actions.append(listentry)
                if "configs" in temp:
                    listentry = 'update_topic'+';'+temp[0]+';'+temp[1]
                    list_of_actions.append(listentry)

    print('.......................')
    print(list_of_actions)
    
    
    # Execute List of Actions Using REST API Calls and retreive data from feature file ...............................

    feature_file = open(feature_file_path)
    read_topics = feature_file.read()
    feature_topics_list = json.loads(read_topics)
    feature_topics_dict = {}
    for i in feature_topics_list:
        feature_topics_dict.update(i)
    print('.......................')
    print('feature_topics_dict----'+str(feature_topics_dict))
    feature_file.close()
'''
    for i in list_of_actions:
        data = i.split(';')
        length_of_list = len(data)
        for j in data:
            
        action = i.split(';')[0]
        #property = i.split(';')[2]
        for x,y in feature_topics_dict.items():
            if x == 'topic':
                print ('in logic'+str(x))

'''

if __name__ == '__main__':
    main()
