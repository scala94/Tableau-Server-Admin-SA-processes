import requests # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import math
from datetime import datetime, date
import win32com.client as client
import pandas as pd
import os
import re
import ast
import psycopg2
import pandas as pd


def setup():
    global verifySsl, VERSION, xmlns
    
    verifySsl = False
    #Tableau Server version nr.
    VERSION = '3.8'
    xmlns = {'t': 'http://tableau.com/api'}


#Configurations for different ECB Tableau servers
    

class ApiCallError(Exception):
    pass

class UserDefinedFieldError(Exception):
    pass

def _encode_for_display(text):
    """
    Encodes strings so they can display as ASCII in a Windows terminal window.
    This function also encodes strings for processing by xml.etree.ElementTree functions.

    Returns an ASCII-encoded version of the text.
    Unicode characters are converted to ASCII placeholders (for example, "?").
    """
    return text.encode('ascii', errors="backslashreplace").decode('utf-8')


def _check_status(server_response, success_code):
    """
    Checks the server response for possible errors.

    'server_response'       the response received from the server
    'success_code'          the expected success code for the response
    Throws an ApiCallError exception if the API call fails.
    """
    if server_response.status_code != success_code:
        parsed_response = ET.fromstring(server_response.text)

        # Obtain the 3 xml tags from the response: error, summary, and detail tags
        error_element = parsed_response.find('t:error', namespaces=xmlns)
        summary_element = parsed_response.find('.//t:summary', namespaces=xmlns)
        detail_element = parsed_response.find('.//t:detail', namespaces=xmlns)

        # Retrieve the error code, summary, and detail if the response contains them
        code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
        summary = summary_element.text if summary_element is not None else 'unknown summary'
        detail = detail_element.text if detail_element is not None else 'unknown detail'
        error_message = '{0}: {1} - {2}'.format(code, summary, detail)
        raise ApiCallError(error_message)
    return

def sign_in(session, server, username, password, site=""):
    """
    Signs in to the server specified with the given credentials

    'server'   specified server address
    'name'     is the name (not ID) of the user to sign in as.
               Note that most of the functions in this example require that the user
               have server administrator permissions.
    'password' is the password for the user.
    'site'     is the ID (as a string) of the site on the server to sign in to. The
               default is "", which signs in to the default site.
    Returns the authentication token and the site ID.
    """
    url = server + "/api/{0}/auth/signin".format(VERSION)

    # Builds the request
    xml_request = ET.Element('tsRequest')
    credentials_element = ET.SubElement(xml_request, 'credentials', name=username, password=password)
    ET.SubElement(credentials_element, 'site', contentUrl=site)
    xml_request = ET.tostring(xml_request)

    # Make the request to server
    server_response = session.post(url, data=xml_request, verify=verifySsl) 
    _check_status(server_response, 200)

    # ASCII encode server response to enable displaying to console
    server_response = _encode_for_display(server_response.text)

    # Reads and parses the response
    try:
        parsed_response = ET.fromstring(server_response)
    except Exception as err:
        print("There was an error parsing the server response. This error may be linked to incorrect credentials.")
        raise

    # Gets the auth token and site ID
    token = parsed_response.find('t:credentials', namespaces=xmlns).get('token')
    site_id = parsed_response.find('.//t:site', namespaces=xmlns).get('id')
    user_id = parsed_response.find('.//t:user', namespaces=xmlns).get('id')
    return token, site_id, user_id

def sign_out(session, server, auth_token):
    """
    Destroys the active session and invalidates authentication token.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    """
    url = server + "/api/{0}/auth/signout".format(VERSION)
    
    server_response = session.post(url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
    _check_status(server_response, 204)
    return


def log_file(log):
    logfile_name = datetime.now().strftime("logs/log_%m%d_%H%M%S.txt")
    file = open(logfile_name, "w") 
    file.write(log)
    file.close()
    os.system(logfile_name.replace('/', '\\'))
    error()
    return

"""
def delete_extract_refresh(session, server, auth_token, site_id, extract_list, all_project, all_workbooks, all_datasources, sched_df, log = ''):
     
    page_size = 100
    
    # Query schedules in server
    
    for sch in sched_df:
        try:
            # query extract refresh tasks from schedule
            page_num = 1
            schedule_id = sch['id']
            url = server + "/api/{}/sites/{}/schedules/{}/extracts".format(VERSION, site_id, schedule_id)
            paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)


            server_response = requests.get(url, headers={'x-tableau-auth': auth_token}, verify=False)
            _check_status(server_response, 200)

            xml_response = ET.fromstring(_encode_for_display(server_response.text))
            
            text_sched = re.split('<extracts><|</extract><',server_response.text)
            text_sched_resp = [ast.literal_eval('{'+tr.replace('><',' ').replace('/>',' ').rstrip().replace('" ','", ').replace(' id','_id').replace('=','":').replace(', ',', "').replace('extract_id','"extract_id')+'}') for tr in text_sched if 'extract id' in tr]

            total_sched = int(xml_response.find('t:pagination', namespaces=xmlns).get('totalAvailable'))
            max_page = int(math.ceil(total_sched / page_size))

            # Continue querying if more refresh tasks exist for the schedule
            for page in range(2, max_page + 1):
                page_num+=1
                paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)

                server_response = session.get(paged_url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
                _check_status(server_response, 200)
                xml_response = ET.fromstring(_encode_for_display(server_response.text))

                text_sched = re.split('<extracts><|</extract><',server_response.text)
                text_sched_resp = text_sched_resp + [ast.literal_eval('{'+tr.replace('><',' ').replace('/>',' ').rstrip().replace('" ','", ').replace(' id','_id').replace('=','":').replace(', ',', "').replace('extract_id','"extract_id')+'}') for tr in text_sched if 'extract id' in tr]
        
        except Exception as err:
            log = log + '\n\n ERROR: could not query extract refresh tasks in schedule "{0}"'.format(sch['name'])
            log_file(log)

        for tsr in text_sched_resp:
            if 'workbook_id' in list(tsr.keys()):
                tsr['workbook_name'] = find_workbook(all_workbooks, tsr['workbook_id'])
                if tsr['workbook_name'].strip() in [el['title'] for el in extract_list]:
                    log = log + '\n\nExtract Refresh task (id {}) found in schedule "{}" for workbook {}.\nDELETING TASK'.format(tsr['extract_id'], sch['name'] , tsr['workbook_name'])
                    delete_url = server + '/api/{}/sites/{}/tasks/extractRefreshes/{}'.format(VERSION, site_id, tsr['extract_id'])
                    print(delete_url)
                    try:
                        #server_response = requests.delete(delete_url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
                        log = log + ' ---> DELETED!'
                    except Exception as err:
                        log = log + '\n\nERROR: could not delete task, some problem occurred!'
                        log_file(log)
            elif 'datasource_id'  in list(tsr.keys()):
                tsr['datasource_name'] = find_workbook(all_datasources, tsr['datasource_id'])
                if tsr['datasource_name'].strip() in [el['title'] for el in extract_list]:
                    print(tsr)
                    log = log + '\n\nExtract Refresh task (id {}) found in schedule "{}" for datasource {}.\nDELETING TASK'.format(tsr['extract_id'], sch['name'] , tsr['datasource_name'])
                    delete_url = server + '/api/{}/sites/{}/tasks/extractRefreshes/{}'.format(VERSION, site_id, tsr['extract_id'])
                    try:
                        #server_response = requests.delete(delete_url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
                        log = log + ' ---> DELETED!'
                    except Exception as err:
                        log = log + '\n\nERROR: could not delete task, some problem occurred!'
                        log_file(log)
    return log
"""

def delete_extract_refresh(session, server, auth_token, site_id, extract_failed_list, log = ''):
    
    for efl in extract_failed_list:
            log = log + '\n\nExtract Refresh task (id {}) found for {} {}.\nDELETING TASK'.format(efl['task_id'], efl['object'], efl['title'])
            delete_url = server + '/api/{}/sites/{}/tasks/extractRefreshes/{}'.format(VERSION, site_id, efl['task_id'])
            try:
                server_response = requests.delete(delete_url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
                log = log + ' ---> DELETED!\n\n' + delete_url
            except Exception as err:
                log = log + '\n\nERROR: could not delete task, some problem occurred!'
                log_file(log)
    return log



def find_owners(all_objects, target_object):
    o_found = []
    for obj in all_objects:
        if target_object in obj.get('name'):
            o_found.append(obj.find('t:owner',namespaces = xmlns).get('id'))
    return list(set(o_found))


def query_views(session, server, auth_token, site_id, workbook_id):
    url = server + "/api/{0}/sites/{1}/workbooks/{2}/views".format(VERSION, site_id, workbook_id)

    server_response = session.get(url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    
    views = xml_response.findall('.//t:view', namespaces=xmlns)
    
    return views


def get_objects_owners(session, server, site_id, user_id, auth_token, all_projects, all_workbooks, all_datasources):

    
    all_views = []
    for workbook in all_workbooks:
        if workbook.get('id') != None:
            views_in_workbook = query_views(session, server, auth_token, site_id, workbook.get('id'))
            for viw in views_in_workbook:
                view = {'connection': viw,'workbook': workbook.get('name')}
                all_views.append(view)
    
    projects = []
    workbooks = []
    datasources = []
    views = []
    
    for pro in all_projects:
        if pro != None:
            project_owner = {'object' : 'project', 
                            'name' : pro.get('name'), 
                            'id' : pro.get('id'), 
                            'owner_id' : pro.find('t:owner',namespaces = xmlns).get('id')}
            projects.append(project_owner)
    for wor in all_workbooks:
        if wor != None:
            workbook_owner = {'object' : 'workbook', 
                            'name' : wor.get('name'), 
                            'id' : wor.get('id'), 
                            'owner_id' : wor.find('t:owner',namespaces = xmlns).get('id')}
            workbooks.append(workbook_owner)
    for dat in all_datasources:
        if dat != None:
            datasource_owner = {'object' : 'datasource', 
                            'name' : dat.get('name'), 
                            'id' : dat.get('id'), 
                            'owner_id' : dat.find('t:owner',namespaces = xmlns).get('id')}
            datasources.append(datasource_owner)
    for vie in all_views:
        if vie != None and vie['connection'].find('t:owner',namespaces = xmlns) != None:
            views_owner = {'object' : 'view', 
                            'workbook_name' : vie['workbook'],
                            'name' : vie['connection'].get('name'), 
                            'id' : vie['connection'].get('id'), 
                            'owner_id' : vie['connection'].find('t:owner',namespaces = xmlns).get('id')}
            views.append(views_owner)    
    return projects, workbooks, datasources, views


def query_jobs(session, server, auth_token, site_id, page_size, page_num):
    
    url = "{0}/api/{1}/sites/{2}/jobs".format(server, VERSION, site_id)
    paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)
   
    server_response = requests.get(url, headers={'x-tableau-auth': auth_token}, verify=False)
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    text_response = server_response.text.split('><')
    
    total_items = int(xml_response.find('t:pagination', namespaces=xmlns).get('totalAvailable'))
    max_page = int(math.ceil(total_items / page_size))
    
    # Continue querying if more workbooks exist on the server
    for page in range(2, max_page + 1):
        page_num+=1
        paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)

        server_response = session.get(paged_url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
        _check_status(server_response, 200)
        xml_response = ET.fromstring(_encode_for_display(server_response.text))
        text_response = text_response + server_response.text.split('><')
 
    job_id = []
    job_status = []
    job_created = []
    job_started = []
    job_ended = []
    job_type = []
    
    for tr in text_response:
        if 'backgroundJob' in tr and 'backgroundJobs' not in tr:
            new_line = tr.split(' ')
            job_id.append([jid for jid in new_line if 'id' in jid][0].split('"')[1])
            job_status.append([jid for jid in new_line if 'status' in jid][0].split('"')[1])
            job_created.append([jid for jid in new_line if 'createdAt' in jid][0].split('"')[1])
            job_started.append([jid for jid in new_line if 'startedAt' in jid][0].split('"')[1])
            job_ended.append([jid for jid in new_line if 'endedAt' in jid][0].split('"')[1])
            job_type.append([jid for jid in new_line if 'jobType' in jid][0].split('"')[1])
    df = pd.DataFrame({'id':job_id,'status':job_status,'createdAt':job_created,'startedAt':job_started,
                       'endedAt':job_ended, 'type':job_type})
    return df

def get_all(session, server, auth_token, user_id, site_id, page_size, page_num, obj):
    """
    Gets all_objects from ECB/ESCB Tableau server.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'user_id'       ID of user with access to workbook
    'site_id'       ID of the site that the user is signed into
    'obj'           object to be retrieved: workbook, datasource, project, view
    """
    
    if obj == 'workbook':
        url = server + "/api/{0}/sites/{1}/users/{2}/workbooks".format(VERSION, site_id, user_id)
    else:
        url = server + "/api/{0}/sites/{1}/".format(VERSION, site_id) + obj + 's'

    paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)

    server_response = session.get(url,data=None,  headers={'x-tableau-auth': auth_token}, verify=verifySsl) #, verify=verifySsl
    _check_status(server_response, 200) #Function defined above
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    
    items = xml_response.findall('.//t:' + obj, namespaces=xmlns) #Search XML for workbook data
    
    # Used to determine if more requests are required to find all workbooks on server
    total_items = int(xml_response.find('t:pagination', namespaces=xmlns).get('totalAvailable'))
    max_page = int(math.ceil(total_items / page_size))
    
    # Continue querying if more workbooks exist on the server
    for page in range(2, max_page + 1):
        page_num+=1
        paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)

        server_response = session.get(paged_url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
        _check_status(server_response, 200)
        xml_response = ET.fromstring(_encode_for_display(server_response.text))
        #Search XML server response (xml_response) for relevant data - workbooks
        items.extend(xml_response.findall('.//t:' + obj, namespaces=xmlns))
    
    return items

 
def find_workbook(all_workbooks, workbook_id):
    w_found = []
    for work in all_workbooks:
        if work.get('id') == workbook_id:
            w_found.append(work)
    
    if len(w_found) == 1:
        return w_found[0].get('name')
        

def postgresql(password, host, query):
    """ Querying projects with missing Project Leaders"""
    
    try:
        connection = psycopg2.connect(database='workgroup', user='readonly', password=password, host=host, port=8060)
        cur = connection.cursor()
        cur.execute(query)
        row = cur.fetchone()
        df = [row]
     
        while row is not None:
            df.append(row)
            row = cur.fetchone()
        cur.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if connection is not None:
            connection.close()
    df = pd.DataFrame(df)
    df["Server"] = host
    df = df.drop_duplicates()
    return df


def five_days_errors(password, host):
    #query = "select id,args,title, created_at,started_at,completed_at,job_type,job_name,notes from _background_tasks where finish_code =1 and job_name in ('Refresh Extracts','Increment Extracts')"
    query = "select id,args,title, created_at,started_at,completed_at,job_type,job_name,notes,finish_code from _background_tasks where job_name in ('Refresh Extracts','Increment Extracts')"
    df = postgresql(password, host, query)
    names = ['id','args','title', 'created_at','started_at','completed_at','job_type','job_name','notes','finish_code','server']
    df.columns = names
    df['items'] = [i.split('\n-')[1] for i in list(df['args'])]
    df['items_id'] = [i.split('\n-')[2] for i in list(df['args'])]
    df = df[['id','items','title', 'items_id','created_at','started_at','completed_at','job_type','job_name','notes','finish_code','server']]
    df['date'] = [str(i.day)+'-'+str(i.month)+'-'+str(i.year) for i in list(df['completed_at'])]
    df = df.sort_values(by=['items_id', 'completed_at'])
    
    error_df = df[(df['finish_code'] == 1)]
    unique_items = list(set(list(error_df['items_id'])))
    error_df_list = []
    for ui in unique_items:
        data = df[(df['items_id']) == ui]
        #data = data.groupby('date').tail(1)
        error_df_list.append(data)
    
    for edl in range(len(error_df_list)):
        delta_days = []
        pivot_id = ''
        for i in range(len(error_df_list[edl])):
            if error_df_list[edl].iloc[i]['items_id'] == pivot_id:
                if str(error_df_list[edl].iloc[i]['completed_at']) != 'NaT':
                    l_date = date(error_df_list[edl].iloc[i]['completed_at'].year, error_df_list[edl].iloc[i]['completed_at'].month, error_df_list[edl].iloc[i]['completed_at'].day)
                    delta = l_date - f_date
                    delta_days.append(int(delta.days))
                else:
                    delta_days.append(None)
            else:
                delta_days.append(0)
            pivot_id = error_df_list[edl].iloc[i]['items_id']
            f_date = date(error_df_list[edl].iloc[i]['completed_at'].year, error_df_list[edl].iloc[i]['completed_at'].month, error_df_list[edl].iloc[i]['completed_at'].day)
        error_df_list[edl]['delta'] = delta_days
        
        cum_delta = []
        counter = 0
        for i in range(len(error_df_list[edl])):
            if error_df_list[edl].iloc[i]['finish_code'] == 0:
                cum_delta.append(0)
                counter = 0
            else:
                if counter == 0:
                    counter = 1
                    cum_delta.append(1)
                else:
                    cum_delta.append(counter + int(error_df_list[edl].iloc[i]['delta']))
                    counter  = counter + int(error_df_list[edl].iloc[i]['delta'])
        error_df_list[edl]['cum_delta'] =  cum_delta
            
    five_days_error = []
    for x in error_df_list:
        if list(x['cum_delta'])[len(x)-1] > 5:
            five_days_error.append(x)
            
    return five_days_error


def user_id2name(session, server, auth_token, site_id, target_user_id):
    """
    Maps user ID to the respective user name on the server

    'server'               specified server address
    'auth_token'           authentication token that grants user access to API calls
    'site_id'              ID of the site that the user is signed into
    'target_user_id'       ID of user for which to query name
    """
    # Error handling in case user doens't exist is not necessary, because Tableau does not allow the deletion of users 
    # (even if the license expired) as long as the user has content on the server
    #Check for NaN values or other erroneous inputs
    if (not isinstance(target_user_id, str)):
        return (None, None)
    
    url = server + "/api/{0}/sites/{1}/users/{2}?fields=name".format(VERSION, site_id, target_user_id)
    
    server_response = session.get(url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))    
    
    #Creating tuple of name and e-mail from dictionary
    name = xml_response[0].get('name')       
    return name


def get_project_leader(project_name, all_projects, server, site_id, auth_token, session, df_groups, log):
    
    pfound =[project for project in all_projects if project.get('name') == project_name]
    project_id = pfound[0].get('id')
    
    url=server + "/api/" + VERSION + "/sites/" + site_id + "/projects/" + project_id + "/permissions"

    server_response = session.get(url, data=None , headers={'x-tableau-auth': auth_token}, verify=verifySsl) 
    _check_status(server_response, 200)
    
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    
    permissions = xml_response.findall('.//t:granteeCapabilities', namespaces=xmlns)
    leads_users = []
    leads_groups = []
    
    for permission in permissions:
        perm = permission.findall('.//t:capability', namespaces=xmlns)[0].get('name')
        
        if 'Project' in perm and 'Leader' in perm:
            
            user = permission.find('.//t:user', namespaces=xmlns)
            group = permission.find('.//t:group', namespaces=xmlns)
            if user != None:
                leads_users.append(user)
            if group != None:
                leads_groups.append(group)

    l_users = []
    l_groups = []

    for lgroup in leads_groups:
        try:
            l_groups.append({'id': lgroup.get('id')})
            uing = get_users_in_group(df_groups, lgroup.get('id'))
            leads_users = leads_users + uing
        
        except Exception as err:
            log = log + "\n\nERROR: your user is not authorized to query group '{0}' in server {1}, so no email was sent.\n Admin privilegies are required!".format(lgroup.get('id'), server)
            l_groups.append(None)
            log_file(log)

    for luser in leads_users:
        try:
            if type(luser) == dict:
                l_users.append(luser)
            else:
                l_user = user_id2name(session, server, auth_token, site_id, luser.get('id'))
                l_users.append({'name': l_user, 'id': luser.get('id')})
        except Exception as err:
            log = log + "\n\nERROR: your user is not authorized to query user '{0}' in server {1}, so no email was sent.\nAdmin privilegies are required!".format(luser.get('id'),server)
            l_users.append(None)
            log_file(log)

    l_users = [i for i in l_users if i != None]
    l_users = [dict(y) for y in set(tuple(x.items()) for x in l_users)]
    return l_users, l_groups


def get_users_in_group(df_groups, group_id):
    """
    Get all the users in the group using group id
 

    if page_size == 0:
        url = server + "/api/{0}/sites/{1}/groups/{2}/users".format(VERSION, site_id, group_id)
    else:
        url = server + "/api/{0}/sites/{1}/groups/{2}/users?pageSize={3}&pageNumber={4}".format(VERSION, site_id, group_id, page_size, page_number)

    server_response = requests.get(url, headers={'x-tableau-auth': auth_token}, verify = False)
    #_check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    users = xml_response.findall('.//t:user', namespaces=xmlns)
    return users
    """
    df = df_groups[(df_groups[0] == group_id)][[1,2]]
    df = [{'name' : list(df[1])[i].lower(), 'id' : list(df[2])[i]} for i in range(len(df))]
    for d in range(len(df)):
        if 't-' in df[d]['name']:
            df[d]['name'] = df[d]['name'][4:(len(df[d]['name'])-1)]
    return df


def extract_refresh_delete(username, password, server_config, list_failed_extract, df_groups, log = ''):
    
    """
    delete extract refresh tasks and output the session log text
    
    'username'              Tableau ECB/ESCB username (Admin)
    'password'              Tableau ECB/ESCB password (Admin)
    'server_config'         server from config()
    'list_failed_extract'   list of items you want to delete the extract refresh task from
    """
    
    setup()
    server = server_config['server']
    print("Processing server: {0}".format(server))
    # Create session object for use throughout script
    # Session object also disallows for system-wide environment variables (e.g. http_proxy) that may interfere with connection
    # For ESCB domain, the session needs to fetch necessary authentication cookies
    # by authenticating on IAM portal
    data = {'userid':username,
    'app':'TABLEAU',
    'password':password,
    'submit':'Login'}
    session = requests.Session()
    session.trust_env = False
    if 'escb.eu' in server:
        #the IAM portal sometimes does not respond, so the connection attempt will be executed 5 times until it fails
        iam_auth_counter = 0
        while 'OAMAuthnHintCookie' not in session.cookies.get_dict() and iam_auth_counter < 5:
            session.get(server, verify=False, allow_redirects=True)
            auth_link = session.post(server_config['iam'], data=data, verify=False, allow_redirects=False)
            session.get(url=auth_link.headers['Location'], verify=False, allow_redirects=False)
            iam_auth_counter += 1
        if 'OAMAuthnHintCookie' not in session.cookies.get_dict():
            print('Connection to IAM portal not possible, please try again later.')
    
    ##### STEP 1: Sign in #####

    log = log + "\n\n ---------- {0} server ---------------\nSigning in as {1}".format(server,username)
    try:
        auth_token, site_id, user_id = sign_in(session, server, username, password)
        log = log + " ---> succeded"
    except Exception as err:
        log = log + "\n\nERROR: could not sign in server {0}".format(server)
        log_file(log)

    page_size=100 # maximum number of items per page
    page_num=1

    try:
        all_projects = get_all(session, server, auth_token, user_id, site_id, page_size, page_num, 'project')
        all_workbooks = get_all(session, server, auth_token, user_id, site_id, page_size, page_num, 'workbook')
        all_datasources = get_all(session, server, auth_token, user_id, site_id, page_size, page_num, 'datasource')
    except Exception as err:
        log = log + '\n\n ERROR: could not query objects in the server, some problem occurred'
        log_file(log)
    
    for lfe in list_failed_extract:
        if lfe['object'].lower() == 'workbook':
            try:
                owners_id = find_owners(all_workbooks, lfe['title'])
                owners_names = []
                item = [wk for wk in all_workbooks if  lfe['title'] in wk.get('name')][0]
                for oi in owners_id:
                    owners_names.append(user_id2name(session, server, auth_token, site_id, oi))
            except Exception as err:
                log = log + '\n\nERROR: problem in searching for owners for workbook {0}'.format(lfe['title'])
                log_file(log)
        elif lfe['object'].lower() == 'datasource':
            try:
                owners_id = find_owners(all_datasources, lfe['title'])
                owners_names = []
                item = [ds for ds in all_datasources if lfe['title'] in ds.get('name')][0]
                for oi in owners_id:
                    owners_names.append(user_id2name(session, server, auth_token, site_id, oi))
            except Exception as err:
                log = log + '\n\nERROR: problem in searching for owners for datasource {0}'.format(lfe['title'])
                log_file(log)

        log = log + '\n\nFollowing owners found for {0} {1}: {2}'.format(lfe['object'].lower(), lfe['title'], ', '.join(owners_names))
        
        try:
            pivot_pro = [pr for pr in all_projects if item.find('.//t:project', namespaces=xmlns).get('id') == pr.get('id')]
            while pivot_pro[0].get('parentProjectId') != None:
                pivot_pro = [pr for pr in all_projects if pr.get('id') == pivot_pro[0].get('parentProjectId')]
            pivot_pro = pivot_pro[0].get('name')
        except Exception as err:
            log = log + '\n\nERROR: could not find main project for {0} {1}'.format(lfe['object'].lower(), lfe['title'])
            log_file(log)

        log = log + '\nsearching Project Leaders in main project {0}'.format(pivot_pro)

        try:
            l_users, l_groups = get_project_leader(pivot_pro, all_projects, server, site_id, auth_token, session, df_groups, log)
        except Exception as err:
            log = log + '\n\nERROR: could not find PLs in project {0}'.format(pivot_pro)
            log_file(log)
        
        log = log + '\nFollowing PLs found for project {0}: {1}\n------ Creating email for {2} {3}'.format(pivot_pro, ', '.join([lu['name'] for lu in l_users]),lfe['object'].lower(), lfe['title'])
        
        try:
            print(owners_names,[lu['name'] for lu in l_users])
            extract_refresh_email(owners_names, [lu['name'] for lu in l_users], server, lfe)
        except Exception as err:
            log = log + '\n\nERROR: could prepare the email for failed extract refresh {0}'.format(lfe['title'])
            log_file(log)
    ##### STEP 2: delete failed extract refresh #####
    
    #log = delete_extract_refresh(session, server, auth_token, site_id, list_failed_extract, all_projects, all_workbooks, all_datasources, sched_df, log)
    #log = delete_extract_refresh(session, server, auth_token, site_id, list_failed_extract, log)
    ##### STEP 3: Sign out #####
        
    print("\n7. Signing out and invalidating the authentication token")
    sign_out(session, server, auth_token)
    session.close()
    
    return log

def extract_refresh_email(emails, CCs, server, extract_refresh_failed):
    
    outlook = client.Dispatch("Outlook.Application")
    message  = outlook.CreateItem(0)
    
    for i in range(len(emails)):
        if 't-' in emails[i].lower():
            emails[i] = emails[i].lower()[4:len(emails[i])]
        elif emails[i].lower()[0:2] == 'eu' and len(emails[i]) > 8:
            emails[i] = emails[i].lower()[2:len(emails[i])]
        else:
            emails[i] = emails[i].lower()
    
    for ii in range(len(CCs)):
        if 't-' in CCs[ii].lower():
            CCs[ii] = CCs[ii].lower()[4:len(CCs[ii])]
        elif CCs[ii].lower()[0:2] == 'eu' and len(CCs[ii]) > 8:
            CCs[ii] = CCs[ii].lower()[2:len(CCs[ii])]
        else:
            CCs[ii] = CCs[ii].lower()
    CCs = list(set(CCs).difference(emails)) 

    message.To = '; '.join(list(set(emails)))
    message.CC = '; '.join(list(set(CCs)))
    message.BCC = ""
    message.Subject = "FOR YOUR INFORMATION : Refresh extracts failed on Tableau Server"
    
    Body = """
&nbsp_____________________________________________________________________________________________<br>
{0}{0}{0}{0}{0}{0} <img src="{5}" alt=Move workbook back to project><br>
&nbsp_____________________________________________________________________________________________<br>
<br>
{0} Dear Colleague(s),<br>
<br>
{0} We have seen that below extract(s) is/are scheduled scheduled to be refreshed daily, but failed lately. <br>
<br>
{0}<a href='{1}/#/{2}s/{3}'>{4}</a> <br>
<br>
{0} Since the refresh of an extract is a very heavy process for Tableau and the refresh stopped working, <br>
<br>
{0} please be informed that your extract(s) is/are  unscheduled automatically. <br>
<br>
{0} As usual, you can reschedule them again once the issues are solved. <br>
<br>
{0} Best regards, <br>
{0} Tableau Support team <br>""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp', server, extract_refresh_failed['object'].lower(), extract_refresh_failed['id'],extract_refresh_failed['title'],os.getcwd()+"\\Tableau.jpg")

    message.HTMLBody = Body
    message.Display()

