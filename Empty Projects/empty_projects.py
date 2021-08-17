# -*- coding: utf-8 -*-
"""
Created on Tue Mar 23 09:24:25 2021

@author: scalabr
"""

import requests # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import math
from datetime import datetime
import win32com.client as client
import pandas as pd
import os
import psycopg2

def setup():
    global verifySsl, VERSION, xmlns
    
    verifySsl = False
    #Tableau Server version nr.
    VERSION = '3.4'
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

def get_empty_projects(session, server, auth_token, site_id, user_id, page_size, page_num):
    
    """
    return the list of empty projects and hierarchy
    
    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'site_id'       ID of the site that the user is signed into
    'user_id'       ID of user with access to workbook
    'proj_name'     name of project from which one wants to find the objects
    """

    # retrieve all objects
    all_projects = get_all(session, server, auth_token, user_id, site_id, page_size, page_num, 'project')
    all_workbooks = get_all(session, server, auth_token, user_id, site_id, page_size, page_num, 'workbook')
    all_datasources = get_all(session, server, auth_token, user_id, site_id, page_size, page_num, 'datasource')
    
    # find empty_projects
    empty_projects = []
    for proj in all_projects:
        workbooks_in_project = [wb.get('name') for wb in all_workbooks if wb.find('.//t:project', namespaces=xmlns).get('id') == proj.get('id')]
        datasources_in_project = [ds.get('name') for ds in all_datasources if ds.find('.//t:project', namespaces=xmlns).get('id') == proj.get('id')]
        subprojects_in_project = [pj.get('name') for pj in all_projects if pj.get('parentProjectId') ==  proj.get('id')]
        
        if len(workbooks_in_project) + len(datasources_in_project) + len(subprojects_in_project) == 0:
            if proj.get('parentProjectId') == None:
                empty_projects.append(proj)
        
    return empty_projects, all_projects
    

def get_all(session, server, auth_token, user_id, site_id, page_size, page_num, obj):
    """
    Gets all_objects from ECB/ESCB Tableau server.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    'user_id'       ID of user with access to workbook
    'site_id'       ID of the site that the user is signed into
    'obj'           object to be retrieved: workbook, datasource, project, 
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
            logfile_name = datetime.now().strftime("logs/log_%m%d_%H%M%S.txt")
            file = open(logfile_name, "w") 
            file.write(log) 
            file.close()
            os.system(logfile_name.replace('/', '\\'))
            error()

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
            logfile_name = datetime.now().strftime("logs/log_%m%d_%H%M%S.txt")
            file = open(logfile_name, "w") 
            file.write(log) 
            file.close()
            os.system(logfile_name.replace('/', '\\'))
            error()       
        
    l_users = [i for i in l_users if i != None]
    l_users = [dict(y) for y in set(tuple(x.items()) for x in l_users)]
    return l_users, l_groups


def user_to_email(users_list):
    datauser = pd.read_excel("lab_prj_disc_tableau.xlsx")
    emails_list = []
    for username in users_list:
        if 'sa_' in username:
             username = username.replace('sa_','')
        emails_list.append(list(datauser['Email Address'][datauser['Username'] == username])[0])
    return emails_list


def empty_projects(username, password, server_config, df_groups, log = ''):
    
    
    """
    return the list of empty projects and hierarchy
    
    'username'        Tableau ECB/ESCB username (Admin)
    'password'        Tableau ECB/ESCB password (Admin)
    'server_config'   from config()
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
    log = log + "\n1. Signing in as " + username
    try:
        auth_token, site_id, user_id = sign_in(session, server, username, password)
        log = log + " ---> succeded\n\n"
    except Exception as err:
        log = log + "\n\nERROR: could not sign in server {0}".format(server)
        logfile_name = datetime.now().strftime("logs/log_%m%d_%H%M%S.txt")
        file = open(logfile_name, "w") 
        file.write(log) 
        file.close()
        os.system(logfile_name.replace('/', '\\'))
        error()

    page_size=100 # maximum number of items per page
    page_num=1
    
    ##### STEP 2: retrieve name of empty projects with hierarchies #####
    try:
        empty_projects, all_projects = get_empty_projects(session, server, auth_token, site_id, user_id, page_size, page_num)
    except Exception as err:
        log = log + "\n\nERROR: could not retrieve empty projects, some problem incurred in the request {0}".format(server)
        file = open(datetime.now().strftime("logs/log_%m%d_%H%M%S.txt"), "w") 
        file.write(log) 
        file.close()
        error()
    log = log + "Empty Projects:\n"
    for empr in empty_projects:
        log = log + "- " + empr.get('name') + "\n"            

    ##### STEP 3: retrieve project leader id for every project (in case is not found, find the closest in hierarchy) #####
    
    em_projects = []

    for empro in empty_projects:
        info = {'name': empro.get('name'), 'id': empro.get('id')}
        try:
            users, groups =  get_project_leader(empro.get('name'), all_projects, server, site_id, auth_token, session, df_groups, log)
            info['lead_users'] = users
            info['lead_groups'] = groups
            info['emails'] = [us['name'] for us in users]
            em_projects.append(info)

        except Exception as err:
            print('             problem incurred with project' + empro.get('name'))
            log = log + "\nERROR: your user is not authorized to query users for project '{0}' in server {1}, so no email was sent.\n Admin privilegies are required!\n".format(empro.get('name'),server)
            logfile_name = datetime.now().strftime("logs/log_%m%d_%H%M%S.txt")
            file = open(logfile_name, "w") 
            file.write(log) 
            file.close()
            os.system(logfile_name.replace('/', '\\'))
            error()
        
    ##### STEP 3: Sign out #####
        
    print("\n7. Signing out and invalidating the authentication token")
    sign_out(session, server, auth_token)
    session.close()
    
    return em_projects, log


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


def empty_projects_email(emails, proj_name, server, proj_num, deadline):
    
    outlook = client.Dispatch("Outlook.Application")
    
    for i in range(len(emails)):
        if 't-' in emails[i].lower():
            emails[i] = emails[i].lower()[4:len(emails[i])]
        elif emails[i].lower()[0:2] == 'eu' and len(emails[i]) > 8:
            emails[i] = emails[i].lower()[2:len(emails[i])]
        else:
            emails[i] = emails[i].lower()

    message.To = '; '.join(list(set(emails)))
    message.BCC = ""
    message.Subject = "FOR YOUR ACTION: Tableau empty projects in Tableau Server"
    
    Body = """
&nbsp_____________________________________________________________________________________________<br>
{0}{0}{0}{0}{0}{0} <img src="{2}" alt=Move workbook back to project><br>
&nbsp_____________________________________________________________________________________________<br>
<br>
{0} Dear Tableau Project Leaders, <br>

<br>
{0} We are currently performing a clean-up of all our Tableau Servers and we have observed that the <br>
{0} project you own has no content. We are kindly asking you if the mentioned project can be deleted  <br>
{0} from Tableau server or the item is still needed. <br>
<br>
{0} <b>!</b> Please be informed that in case of no reply to this email, we will consider that is ok from <br>
{0} your side to <b>permanently delete</b> the project from Tableau which will happen on <b>{1} <br>
{0} without any further announcement.</b> <br>
&nbsp_____________________________________________________________________________________________<br>
<br>
{0}{0}{0}{0}{0}{0} <b>Empty project(s) to be decommissioned on {1}:</b><br>
<br>""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp', deadline, os.getcwd()+"\\Tableau.jpg")
    if server == 'https://a-tableau.ecb.de/':
        Body = Body + """{0} <a href='https://a-tableau.ecb.de/'>Tableau ECB Acceptance Server</a> <br>
        {0}{0} <a href='https://a-tableau.ecb.de/#/projects/{2}'> {1} </a> <br>""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp', proj_name, proj_num)
    elif server == 'https://a-tableau.escb.eu':
        Body = Body + """{0} <a href='https://a-tableau.escb.eu/'>Tableau ESCB Acceptance Server</a> <br>
        {0}{0} <a href='https://a-tableau.escb.eu/#/projects/{2}'> {1} </a> <br>""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp', proj_name, proj_num)
    elif server == 'https://tableau.ecb.de/':
        Body = Body + """{0} <a href='https://tableau.ecb.de/'>Tableau ECB Production Server</a> <br>
        {0}{0} <a href='https://tableau.ecb.de/#/projects/{2}'> {1} </a> <br>""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp', proj_name, proj_num)
    elif server == 'https://tableau.escb.eu':
        Body = Body + """{0} <a href='https://tableau.escb.eu/'>Tableau ESCB Production Server</a> <br>
        {0}{0} <a href='https://tableau.escb.eu/#/projects/{2}'> {1} </a> <br>""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp', proj_name, proj_num)        
    Body = Body + """<br>
&nbsp_____________________________________________________________________________________________<br>
<br>
{0} Best Regards, <br>
{0} Tableau Support Team <br>""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp')
    message.HTMLBody = Body
    message.Display()
    
