from github import Github
from deepdiff import DeepDiff
from datetime import datetime
from subprocess import PIPE

import click
import json
import logging
import os
import string
import subprocess
import re
import requests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

featuretopics_file_path = 'C:\\Users\\venka\\Desktop\\KafkaAutomation\\revised\\featuretopics.json'
sourcetopics_file_path = 'C:\\Users\\venka\\Desktop\\KafkaAutomation\\revised\\sourcetopics.json'
featureacls_file_path ='C:\\Users\\venka\\Desktop\\KafkaAutomation\\revised\\featureacls.json'
sourceacls_file_path ='C:\\Users\\venka\\Desktop\\KafkaAutomation\\revised\\sourceacls.json'
connector_file_path ='C:\\Users\\venka\\Desktop\\KafkaAutomation\\revised\\connector-src.json'

def readfile(path):  #Open file read contents and return dictonary
    try:
        file = open(path)
        contents = file.read()
        contents_list = json.loads(contents)
        temp_dict = {}
        for index in contents_list:
            temp_dict.update(index)
        file.close()
        return temp_dict
    except KeyError as ke:
        logger.error(ke)

def compare_files(file1, file2): # compare files and return the changes in the form of Action File
    diff = DeepDiff(file1, file2, ignore_order=True)
    list_of_actions = []
    for change_key,change_value in diff.items():
        if (change_key == 'dictionary_item_added'):
            temp_list = re.findall(r"\['(.*?)'\]", str(change_value))
            for item in temp_list:
                listentry = 'ADD' + ';' + str(item)
                list_of_actions.append(listentry)

        if (change_key == 'dictionary_item_removed'):
            temp_list = re.findall(r"\['(.*?)'\]", str(change_value))
            for item in temp_list:
                listentry = 'DELETE' + ';' + str(item)
                list_of_actions.append(listentry)

        if (change_key == 'values_changed'):
            for key,value in change_value.items():
                #print("Key : "+str(key[4:]))
                temp = re.findall(r"\['(.*?)'\]", str(key))
                if "partitions_count" in temp:
                    listentry = 'UPDATE' + ';' + temp[0] + ';' + temp[1]
                    list_of_actions.append(listentry)
                if "configs" in temp:
                    count = re.findall(r"\d",key)
                    property_index = int(count[0])
                    #print("count : " + str(count[0]))
                    if property_index == 0:
                        listentry = 'UPDATE' + ';' + temp[0] + ';' + temp[1] + ';' + 'cleanup.policy'
                        list_of_actions.append(listentry)
                    if property_index == 1:
                        listentry = 'UPDATE' + ';' + temp[0] + ';' + temp[1] + ';' + 'compression.type'
                        list_of_actions.append(listentry)
                    if property_index == 2:
                        listentry = 'UPDATE' + ';' + temp[0] + ';' + temp[1] + ';' + "retention.ms"
                        list_of_actions.append(listentry)
                    if property_index == 3:
                        listentry = 'UPDATE' + ';' + temp[0] + ';' + temp[1] + ';' + "max.message.bytes"
                        list_of_actions.append(listentry)

    return(list_of_actions)

def parse_dictonary(dictionary,actionfile_item):
    actionfile_tasks = actionfile_item.split(';')
    payload= {}
    if len(actionfile_tasks) == 2:
            topic_name = actionfile_tasks[1]
            if topic_name in dictionary.keys():
                payload = dictionary.get(topic_name)
            else:
                payload ={'Name to be deleted':actionfile_tasks[1]}

    if  len(actionfile_tasks) == 3:
        topic_name= actionfile_tasks[1]
        partition_name = actionfile_tasks[2]
        if topic_name in dictionary.keys():
                topic_dict = dictionary.get(topic_name)
                for prop_name, prop_value in topic_dict.items():
                    if prop_name == 'partitions_count':
                        payload = {prop_name:prop_value}

    if  len(actionfile_tasks) == 4:
        topic_name= actionfile_tasks[1]
        partition_name = actionfile_tasks[2]
        config_prop_name = actionfile_tasks[3]
        if topic_name in dictionary.keys():
                topic_dict = dictionary.get(topic_name)
                for prop_name, prop_value in topic_dict.items():
                    if prop_name == 'configs':
                        listofProperties = prop_value
                        for index in listofProperties:
                            if index.get('name') == config_prop_name:
                                config_prop = index.get('name')
                                config_prop_value = index.get('value')
                                payload = {config_prop: config_prop_value}
    return (payload)

def main():
    featuretopic_dict = readfile(featuretopics_file_path)
    sourcetopic_dict = readfile(sourcetopics_file_path)
    featureacl_dict = readfile(featureacls_file_path)
    sourceacl_dict = readfile(sourceacls_file_path)
    connector_dict = readfile(connector_file_path)

    topics_action_file = compare_files(sourcetopic_dict,featuretopic_dict)
    topics_payload ={}
    for index in topics_action_file:
        topic_payload = parse_dictonary(featuretopic_dict, index)
        print(topic_payload)
    print(topics_action_file)

    acls_action_file = compare_files(sourceacl_dict, featureacl_dict)
    acl_payload ={}
    print(acls_action_file)
    for index in acls_action_file:
        acl_payload = parse_dictonary(featureacl_dict, index)
        print("acl_payload :"+str(acl_payload))

if __name__ == '__main__':
    main()
