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
HEADERS = {'Content-type': 'application/json', 'Accept': 'application/json'}
REST_BASIC_AUTH_USER = os.getenv('REST_BASIC_AUTH_USER')
REST_BASIC_AUTH_PASS = os.getenv('REST_BASIC_AUTH_PASS')
CONNECT_BASIC_AUTH_USER = os.getenv('CONNECT_BASIC_AUTH_USER')
CONNECT_BASIC_AUTH_PASS = os.getenv('CONNECT_BASIC_AUTH_PASS')
gitdiff = os.getenv('gitdiff')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

featuretopics_file_path = 'application1/topics/featuretopics.json'
sourcetopics_file_path = 'application1/topics/sourcetopics.json'
featureacls_file_path ='application1/acls/featureacls.json'
sourceacls_file_path ='application1/acls/sourceacls.json'
#connector_file_path ='application1/connectors/connector-src.json'

topics_url= 'https://cvlappxd30102.silver.com:8082/v3/clusters/i9xuuwhiQTidT34al14bJA/topics/'
acls_url = 'https://cvlappxd30102.silver.com:8082/v3/clusters/i9xuuwhiQTidT34al14bJA/acls/'

def executeurl_topics(action_dict):
    resource = action_dict.get('resource')
    resource_name = action_dict.get('resource_name')
    action = action_dict.get('action')
    url = action_dict.get('url')
    payload  =  action_dict.get('payload')
    get_response = requests.get(url+resource_name+'/',auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), verify='trustore.pem')
    payload_json = json.dumps(payload)
    if get_response.status_code != 200:
        print("No Such Topic Exists, Ready to add the Topic : "+str(resource_name))
        if action == 'add_topics':
            print("request.post add topic")
            response = requests.post(url, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), data=payload_json, headers=HEADERS, verify='trustore.pem')
            print(response.text)
            print(response.status_code)

    if get_response.status_code == 200:
        print("Topic Match Found :"+str(resource_name))
        if action == 'delete_topics':
            print("request.post delete topic")
            response = requests.delete(url+resource_name+'/', auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS),headers=HEADERS, verify='trustore.pem')
            print(response.text)
            print(response.status_code)
        if action == 'update_partition':
            print('request.post update partition')
            response = requests.patch(url+resource_name+'/', auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), data=payload_json, headers=HEADERS, verify='trustore.pem')
            print(response.text)
            print(response.status_code)
        if action == 'update_configs':
            url = url+resource_name+'/'+ "configs:alter/"
            print(url)
            print('request.post update configs')
            response = requests.post(url, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), data=payload_json, headers=HEADERS, verify='trustore.pem')
            print(response.text)
            print(response.status_code)
        if action == 'add_topics':
            print('topic already exist:' +str(resource_name))
        
def executeurl_acl(action_dict):
    resource = action_dict.get('resource')
    resource_name = action_dict.get('resource_name')
    action = action_dict.get('action')
    url = action_dict.get('url')
    payload  =  action_dict.get('payload')
    get_response = requests.get(url,auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), verify='trustore.pem')
    payload_json = json.dumps(payload)
    acl_configs = list(payload.values())
    if get_response.status_code == 200:
        if action == 'add_acls':
            print("request.post add ACL")
            response = requests.post(url, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS), data=payload_json, headers=HEADERS, verify='trustore.pem')
            print(response.text)
            print(response.status_code)
        if action == 'delete_acls':
            print("request.post delete ACL")
            print(payload_json)
            response = requests.delete(url, auth=(REST_BASIC_AUTH_USER, REST_BASIC_AUTH_PASS),params=payload, headers=HEADERS, verify='trustore.pem')
            print(response.text)
            print(response.status_code)    
        

def buildurl(resource):
    if resource == 'topics':
        url = topics_url
    if resource == 'acls':
        url = acls_url
    return (url)

def convert_to_dict(input_file_as_a_list): #Open file read contents and return dictonary
        temp_dict = {}
        for index in input_file_as_a_list:
            temp_dict.update(index)
        return temp_dict

def compare_files(file1, file2,resource_type): # compare files and return the changes in the form of Action File
    diff = DeepDiff(file1, file2, ignore_order=True)
    #print("\n-----------------Source File--------------------\n :" + str(file1))
    #print("\n-----------------Destination File---------------\n  :" + str(file2))
    #print("\n-----Deep Diff of Source & Destination Files----\n"+str(diff))
    list_of_actions = []
    
    for change_key,change_value in diff.items():
        if (change_key == 'dictionary_item_added'):
            temp_list = re.findall(r"\['(.*?)'\]", str(change_value))
            for item in temp_list:
                listentry = resource_type + ';' + 'add_'+str(resource_type) + ';' + str(item)
                list_of_actions.append(listentry)

        if (change_key == 'dictionary_item_removed'):
            temp_list = re.findall(r"\['(.*?)'\]", str(change_value))
            for item in temp_list:
                listentry = resource_type + ';' + 'delete_'+str(resource_type) + ';' + str(item)
                list_of_actions.append(listentry)

        if (change_key == 'values_changed'):
            for key,value in change_value.items():
                #print("Key : "+str(key[4:]))
                temp = re.findall(r"\['(.*?)'\]", str(key))
                if "partitions_count" in temp:
                    listentry = resource_type + ';' + 'update_partition' + ';' + temp[0] + ';' + temp[1]
                    list_of_actions.append(listentry)
                if "configs" in temp:
                    count = re.findall(r"\d",key)
                    property_index = int(count[0])
                    if property_index == 0:
                        listentry = resource_type + ';' + 'update_configs' + ';' + temp[0] + ';' + temp[1] + ';' + 'cleanup.policy'
                        list_of_actions.append(listentry)
                    if property_index == 1:
                        listentry =  resource_type + ';' + 'update_configs' + ';' + temp[0] + ';' + temp[1] + ';' + 'compression.type'
                        list_of_actions.append(listentry)
                    if property_index == 2:
                        listentry =  resource_type + ';' + 'update_configs' + ';' + temp[0] + ';' + temp[1] + ';' + "retention.ms"
                        list_of_actions.append(listentry)
                    if property_index == 3:
                        listentry =  resource_type + ';' + 'update_configs' + ';' + temp[0] + ';' + temp[1] + ';' + "max.message.bytes"
                        list_of_actions.append(listentry)
    print("lsit of actions : "+str(list_of_actions))
    return(list_of_actions)

def parse_dictonary_acls(source_dictionary,feature_dictionary,actionfile_item):
    actionfile_tasks = actionfile_item.split(';')
    resource = actionfile_tasks[0]
    action= actionfile_tasks[1]
    resource_name = actionfile_tasks[2]
    payload= {}
    dictionary ={}
    if action == 'add_acls':
        dictionary = feature_dictionary
    if action == 'delete_acls':
        dictionary = source_dictionary
    payload = dictionary.get(resource_name)
    topic_url = buildurl(resource)
    summary_dict={}
    summary_dict.update({'resource':resource,'action': action, 'resource_name': resource_name, 'payload': payload, 'url': topic_url})
    print(summary_dict)
    return (summary_dict)



def parse_dictonary_topics(dictionary,actionfile_item):
    actionfile_tasks = actionfile_item.split(';')
    resource = actionfile_tasks[0]
    action= actionfile_tasks[1]
    topic_name = actionfile_tasks[2]   
    payload= {}
    if len(actionfile_tasks) == 3:
            if topic_name in dictionary.keys():
                payload = dictionary.get(topic_name)
            else:
                payload ={'Name to be deleted':actionfile_tasks[2]}

    if  len(actionfile_tasks) == 4:
        if topic_name in dictionary.keys():
                topic_dict = dictionary.get(topic_name)
                for prop_name, prop_value in topic_dict.items():
                    if prop_name == 'partitions_count':
                        payload = {prop_name:prop_value}

    if  len(actionfile_tasks) == 5:
        config_prop_name = actionfile_tasks[4]
        if topic_name in dictionary.keys():
                topic_dict = dictionary.get(topic_name)
                for prop_name, prop_value in topic_dict.items():
                    if prop_name == 'configs':
                        listofProperties = prop_value
                        for index in listofProperties:
                            if index.get('name') == config_prop_name:
                                config_prop = index.get('name')
                                config_prop_value = index.get('value')
                                payload = {"data":[{"name":config_prop,"value":config_prop_value}]}
    topic_url = buildurl(resource)
    summary_dict={}
    summary_dict.update({'resource':resource,'action': action, 'resource_name': topic_name, 'payload': payload, 'url': topic_url})
    return (summary_dict)

def main():
    #files = subprocess.run(['git', 'diff', '--name-status', previous_sha, latest_sha], stdout=PIPE, stderr=PIPE).stdout
    files_string = gitdiff
    #files_string = files.decode('utf-8')
    pattern = re.compile(r'([AMD])\s+(.+)')
    files_list = [match.group(1) + ' ' + match.group(2) for match in pattern.finditer(files_string)]
    print("files_list:" + str(files_list))

    current_topics = 'application1/topics/current-topics.json'
    previous_topics = 'application1/topics/previous-topics.json'
    
    current_acls = 'application1/acls/current-acls.json'
    previous_acls = 'application1/acls/previous-acls.json'

    current_topics_command = f"git show HEAD:application1/topics/topics.json > {current_topics}"
    previous_topics_command = f"git show HEAD~1:application1/topics/topics.json > {previous_topics}"

    current_acls_command = f"git show HEAD:application1/acls/acls.json > {current_acls}"
    previous_acls_command = f"git show HEAD~1:application1/acls/acls.json > {previous_acls}"

    sourcetopic_list  =[]
    featuretopic_list =[]
    sourceacl_list = []
    featureacl_list = []
    for file in files_list:
        if "topics.json" in file:
            filename = file.split(" ")[1]
            print(filename)
            current_topics_command = f"git show HEAD:{filename} > {current_topics}"
            previous_topics_command = f"git show HEAD~1:{filename} > {previous_topics}"
            print(current_topics_command)
            print(previous_topics_command)
            
            subprocess.run(current_topics_command, stdout=PIPE, stderr=PIPE, shell=True)
            subprocess.run(previous_topics_command, stdout=PIPE, stderr=PIPE, shell=True)
            try:
                with open(previous_topics, 'r') as previous_topics_file:
                    sourcetopic_list = json.load(previous_topics_file)
                with open(current_topics, 'r') as current_topics_file:
                    featuretopic_list= json.load(current_topics_file)
            except json.decoder.JSONDecodeError as error:
                logger.error(error)

        if "acls.json" in file:
            filename = file.split(" ")[1]
            print(filename)
            current_acls_command = f"git show HEAD:{filename} > {current_acls}"
            previous_acls_command = f"git show HEAD~1:{filename} > {previous_acls}"
            print(current_acls_command)
            print(previous_acls_command)

            subprocess.run(current_acls_command, stdout=PIPE, stderr=PIPE, shell=True)
            subprocess.run(previous_acls_command, stdout=PIPE, stderr=PIPE, shell=True)
            try:
                with open(previous_acls, 'r') as previous_acls_file:
                    sourceacl_list = json.load(previous_acls_file)
                with open(current_acls, 'r') as current_acls_file:
                    featureacl_list = json.load(current_acls_file)
            except json.decoder.JSONDecodeError as error:
                logger.error(error)
     
    sourcetopic_dict = convert_to_dict(sourcetopic_list)
    featuretopic_dict = convert_to_dict(featuretopic_list)
    sourceacl_dict = convert_to_dict(sourceacl_list)
    featureacl_dict = convert_to_dict(featureacl_list)
        
    # Generate Topics Action File
    print("\n**********************************************************************Topics**************************************************\n")
    topics_action_file = compare_files(sourcetopic_dict,featuretopic_dict,'topics')
    execute_topics_list = []
    for index in topics_action_file:
        topic_payload = parse_dictonary_topics(featuretopic_dict, index)
        execute_topics_list.append(topic_payload)
    print("\n---------------TOPICS Action File------------------\n")
    print(execute_topics_list)
    for topic_change_dict in execute_topics_list:
        executeurl_topics(topic_change_dict)

    # Generate ACL Action File
    print("\n****************************************************************ACLs**********************************************************\n")
    execute_acls_list = []
    acls_action_file = compare_files(sourceacl_dict, featureacl_dict,'acls')
    for index in acls_action_file:
        acl_payload = parse_dictonary_acls(sourceacl_dict,featureacl_dict, index)
        execute_acls_list.append(acl_payload)
    print("\n---------------ACLs Action File------------------\n")
    for acl_change_dict in execute_acls_list:
        executeurl_acl(acl_change_dict)
    


if __name__ == '__main__':
    main()
