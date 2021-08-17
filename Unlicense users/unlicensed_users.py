import requests # Contains methods used to make HTTP requests
import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import math
import pandas as pd
import win32com.client as client
import os
from datetime import datetime
import psycopg2


def setup():
    global verifySsl, VERSION, xmlns
    
    verifySsl = False
    #Tableau Server version nr.
    VERSION = '3.4'
    xmlns = {'t': 'http://tableau.com/api'}


#Configurations for different Tableau servers

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


def sign_out(server, auth_token):
    """
    Destroys the active session and invalidates authentication token.

    'server'        specified server address
    'auth_token'    authentication token that grants user access to API calls
    """
    url = server + "/api/{0}/auth/signout".format(VERSION)
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token}, verify=False)
    _check_status(server_response, 204)
    return


def query_views(session, server, auth_token, site_id, workbook_id):
 
    url = server + "/api/{0}/sites/{1}/workbooks/{2}/views".format(VERSION, site_id, workbook_id)
    
    server_response = session.get(url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    
    views = xml_response.findall('.//t:view', namespaces=xmlns)
    
    return views


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


def get_objects_owners(session, server, site_id, user_id, auth_token):

    all_projects = get_all(session, server, auth_token, user_id, site_id, 100, 1, 'project')
    all_workbooks = get_all(session, server, auth_token, user_id, site_id, 100, 1, 'workbook')
    all_datasources = get_all(session, server, auth_token, user_id, site_id, 100, 1, 'datasource')

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


def find_and_remove(session, server,auth_token,site_id,user_id,postgre_data,postgre_unlicensed,df_groups,log=''):
    """
    we loop for each users of the server and if their site role is "unlicesed" then we remove it from the server 
    """
    try:
        projects, workbooks, datasources, views = get_objects_owners(session, server, site_id, user_id, auth_token)
    except Exception as err:
        log = log + '\n\nERROR: could not retrieve objects owners, please check your admin credentials and retry!'
        log_file(log)

    try:
        all_projects = get_all(session, server, auth_token, user_id, site_id, 100, 1, 'project')
        all_workbooks = get_all(session, server, auth_token, user_id, site_id, 100, 1, 'workbook')
        all_datasources = get_all(session, server, auth_token, user_id, site_id, 100, 1, 'datasource')
    except Exception as err:
        log = log + '\n\nERROR: could not retrieve objects in server {0}, please check your admin credentials and retry!'.format(server)
        log_file(log)

    unlicensed_users = []
    for pu in range(len(postgre_unlicensed)):
        unli_us = {'name' : list(postgre_unlicensed[1])[pu], 'user_id' : list(postgre_unlicensed[0])[pu]}
        unli_us['projects_name'] = []
        unli_us['projects_id'] = []
        unli_us['projects_parent'] = {'name':[] , 'PL': []}
        unli_us['workbooks_name'] = []
        unli_us['workbooks_id'] = []
        unli_us['workbooks_project'] = {'name':[] , 'PL': []}
        unli_us['datasources_name'] = []
        unli_us['datasources_id'] = []
        unli_us['datasources_project'] = {'name':[] , 'PL': []}
        #unli_us['views'] = []
        #unli_us['workbook_name'] = []
        
        for pro in projects:
            if unli_us['user_id'] == pro['owner_id']:
                unli_us['projects_name'].append(pro['name'])
                unli_us['projects_id'].append(pro['id'])
        for wor in workbooks:
            if unli_us['user_id'] == wor['owner_id']:
                unli_us['workbooks_name'].append(wor['name'])
                unli_us['workbooks_id'].append(wor['id'])
        for dat in datasources:
            if unli_us['user_id'] == dat['owner_id']:
                unli_us['datasources_name'].append(dat['name'])
                unli_us['datasources_id'].append(dat['id'])
#                      if obj['object'] == 'view':
#                          unli_us['workbook_name'].append(obj['workbook_name'])
        unlicensed_users.append(unli_us)
    
    emm = []
    NoPLtext = []
    unlius_emails = []
    for unlius in unlicensed_users:
        if len(unlius['projects_name']) + len(unlius['workbooks_name']) + len(unlius['datasources_name']) != 0:
            log = log + '\n\n -   unlicensed user ' + unlius['name'] + ' is still owner of the follwing:\n'
            if len(unlius['projects_name']) != 0:
                log = log + '\nPROJECTS:\n-' + '\n-'.join(list([i for i in unlius['projects_name']]))
                if type(log) == tuple:
                    log = ' '.join(log)
            if len(unlius['workbooks_name']) != 0:
                log = log + '\n\nWORKBOOKS:\n-' + '\n-'.join(list([i for i in unlius['workbooks_name']])) 
                if type(log) == tuple:
                    log = ' '.join(log)
            if len(unlius['datasources_name']) != 0:
                log = log + '\n\nDATASOURCES:\n-' + '\n-'.join(list([i for i in unlius['datasources_name']]))           
                if type(log) == tuple:
                    log = ' '.join(log)
            log = log + '\n\nsearching for project leaders:\n'
            
            for proj in unlius['projects_name']:
                try:
                    pivot = [pr for pr in all_projects if pr.get('name') == proj]
                    while pivot[0].get('parentProjectId') != None:
                        pivot = [pr for pr in all_projects if pr.get('id') == pivot[0].get('parentProjectId')]
                    pivot = pivot[0].get('name')
                    unlius['projects_parent']['name'].append(pivot)
                    pro_lead_users, pro_lead_groups = get_project_leader(pivot, all_projects, server, site_id, auth_token, session, df_groups, log)
                    if len(pro_lead_users) == 0:
                        log = log + '\nno PL found for project {0}'.format(proj)
                    else:
                        log = log + '\nfollowing PL(s) found for project {0}: {1}'.format(proj, ''.join(['\n-' + plu['name'] for plu in pro_lead_users]))
                    unlius['projects_parent']['PL'].append(pro_lead_users)

                except Exception as err:
                    log  = log + '\n\nERROR: could not retrieve projects leaders and project groups for project {0} in {1} server, check your admin credentials and retry'.format(proj,server)
                    log_file(log)
                    
            for work in unlius['workbooks_name']:
                try:
                    workbook = [wk for wk in all_workbooks if wk.get('name') == work][0]
                    pivot_wor = [pr for pr in all_projects if workbook.find('.//t:project', namespaces=xmlns).get('id') == pr.get('id')]
                    while pivot_wor[0].get('parentProjectId') != None:
                        pivot_wor = [pr for pr in all_projects if pr.get('id') == pivot_wor[0].get('parentProjectId')]
                    pivot_wor = pivot_wor[0].get('name')
                    unlius['workbooks_project']['name'].append(pivot_wor)
                    wor_lead_users, wor_lead_groups = get_project_leader(pivot_wor, all_projects, server, site_id, auth_token, session, df_groups, log)
                    if len(wor_lead_users) == 0:
                        log = log + '\nno PL found for workbook {0} in main project {1}'.format(work, pivot_wor)
                    else:
                        log = log + '\nfollowing PL(s) found for workbook {0} in main project {1}: {2}'.format(work, pivot_wor, ''.join(['\n-' + wlu['name'] for wlu in wor_lead_users]))
                    unlius['workbooks_project']['PL'].append(wor_lead_users)

                except Exception as err:
                    log  = log + '\n\nERROR: could not retrieve projects leaders and project groups for workbook {0} in {1} server, check your admin credentials and retry'.format(work,server)
                    log_file(log)
                                     
            for data in unlius['datasources_name']:
                try:
                    datasource = [ds for ds in all_datasources if ds.get('name') == data][0]
                    pivot_dat = [pr for pr in all_projects if datasource.find('.//t:project', namespaces=xmlns).get('id') == pr.get('id')]
                    while pivot_dat[0].get('parentProjectId') != None:
                        pivot_dat = [pr for pr in all_projects if pr.get('id') == pivot_dat[0].get('parentProjectId')]
                    pivot_dat = pivot_dat[0].get('name')
                    unlius['datasources_project']['name'].append(pivot_dat)
                    dat_lead_users, dat_lead_groups = get_project_leader(pivot_dat, all_projects, server, site_id, auth_token, session, df_groups, log)
                    if len(dat_lead_users) == 0:
                        log = log + '\nno PL found for datasource {0} in main project {1}'.format(data, pivot_dat)
                    else:
                        log = log + '\nfollowing PL(s) found for datasource {0} in main project {1}: {2}'.format(data, pivot_dat, ''.join(['\n-' + dlu['name'] for dlu in dat_lead_users]))
                    unlius['datasources_project']['PL'].append(dat_lead_users)

                except Exception as err:
                    log  = log + '\n\nERROR: could not retrieve projects leaders and project groups for project {0} in {1} server, check your admin credentials and retry'.format(data,server)
                    log_file(log)

            unique_projects = list(set(unlius['projects_parent']['name'] + unlius['workbooks_project']['name'] + unlius['datasources_project']['name']))
            email_info = {'user_name':unlius['name'],
              'user_id':unlius['user_id'],
              'project_name':[],
              'emails':[],
              'objects':[]}
            
            for up in unique_projects:
                pro_indices = [i for i, xx in enumerate(unlius['projects_parent']['name']) if xx == up]
                wor_indices = [i for i, xx in enumerate(unlius['workbooks_project']['name']) if xx == up]
                dat_indices = [i for i, xx in enumerate(unlius['datasources_project']['name']) if xx == up]
                pr = list(pd.Series(unlius['projects_parent']['PL'])[pro_indices])
                wo = list(pd.Series(unlius['workbooks_project']['PL'])[wor_indices])
                da = list(pd.Series(unlius['datasources_project']['PL'])[dat_indices])
                PL_per_obj = pr + wo + da
                emails = PL_per_obj[0]
                for PL in PL_per_obj:
                    if PL != emails:
                        log = log + '\n\nWARNING: something went wrong when reshaffeling the objects and different PLs have been found for same project. Please check the email'                       
                PLs = [em['name'] for em in emails]

                email_info['project_name'].append(up)
                email_info['emails'].append(PLs)
                all_obj = {'projects': list(pd.Series(unlius['projects_name'])[pro_indices]),
                           'workbooks': list(pd.Series(unlius['workbooks_name'])[wor_indices]),
                           'datasources': list(pd.Series(unlius['datasources_name'])[dat_indices])}
                email_info['objects'].append(all_obj)
                
                if len(PLs) != 0:
                    emm.append("unlicensed_users_email(['{0}'], '{1}', '{2}', '{3}', '{4}', {{'projects':['{5}'], 'workbooks:['{6}'],'datasources':['{7}']}}".format("', '".join(PLs), server, unlius['name'], up, '000', "', '".join(all_obj['projects']),"', '".join(all_obj['workbooks']),"', '".join(all_obj['datasources'])))
                    try:
                        project_number = str(int(postgre_data[postgre_data[1] == up][0]))  
                        log = log + '\n\nPREPARING EMAIL (unlicensed user {0}, project {1}. \nThe email is sent to following PL(s): {2}'.format(unlius['name'],up, ', '.join(list(PLs)))
                        unlicensed_users_email(PLs, server, unlius['name'], up, project_number, all_obj)
                 
                    except Exception as err:
                        project_number = '000'
                        text = '\n\n WARNING No project_number found for project {0} in server {2}, so the email link to that project for user {1} must be corrected manually (now reports "000")'.format(up, unlius['name'],server)
                        unlicensed_users_email(PLs, server, unlius['name'], up, project_number, all_obj)
                 
                else:
                    text = '\n\n WARNING: No PL found for project {0} in server {2}, so no email was prepared (check manually). Unlicensed user {1} still owns objects in the project'.format(up, unlius['name'],server)
                    NoPLtext.append(text)

            unlius_emails.append(email_info)
        else:
            log = log + '\n\n -   Unlicensed user {0} is going to be deleted'.format(unlius['name'])
            """                   
            delete_url=server + "/api/{0}/sites/{1}/users/{2}".format(VERSION, site_id, user.get('id'))
            try:
                server_response = requests.delete(delete_url, headers={'x-tableau-auth': auth_token}, verify=verifySsl)
                log = log + ' ---> USER DELETED!'
            except Exception as err:
                log = log + '\n\nERROR: could not delete unlicensed user {0}. Please check your admin credentials and retry!'.format(unlius['name'])
                log_file(log)
            """
            
    return unlicensed_users, unlius_emails, log, NoPLtext, emm


def log_file(log):
    logfile_name = datetime.now().strftime("logs/log_%m%d_%H%M%S.txt")
    file = open(logfile_name, "w") 
    file.write(log)
    file.close()
    os.system(logfile_name.replace('/', '\\'))
    error()
    return


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


def user_to_email(users_list, datauser):
    emails_list = []
    if users_list != None and users_list != []:
        for username in users_list:
            emails_list.append(datauser['Email Address'][datauser['Username'] == username])
    return emails_list


def unlicensed_users_email(emails, server, user_name, proj_name, proj_num, proj_objects):
    
    outlook = client.Dispatch("Outlook.Application")
    send_account = None
    
    for i in range(len(emails)):
        if 't-' in emails[i].lower():
            emails[i] = emails[i].lower()[4:len(emails[i])]
        elif emails[i].lower()[0:2] == 'eu' and len(emails[i]) > 8:
            emails[i] = emails[i].lower()[2:len(emails[i])]
        else:
            emails[i] = emails[i].lower()
			
    message  = outlook.CreateItem(0)
    message.To = '; '.join(list(set(emails)))
    message.BCC = ""
    message.Subject = "FOR YOUR ACTION : Removing unlicensed users from Tableau Server"
    
    Body = """
&nbsp_____________________________________________________________________________________________<br>
{0}{0}{0}{0}{0}{0} <img src="{2}" alt=Move workbook back to project><br>
&nbsp_____________________________________________________________________________________________<br>
<br>
{0} Dear Project Leader(s),<br>
<br>
{0} In accordance with our policy to keep the Tableau servers “clean”, we noticed that the user <b>*{1}*</b> <br>
{0} is no longer active and will be removed from the servers. <br>
{0} However, the user still owns content published on : <br><br>""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp', user_name, os.getcwd()+"\\Tableau.jpg")

    Body = Body + """{0} <a href='{3}'>Tableau Server</a> in the project <a href='{3}/{2}'>{1}</a> <br>{0}{0} The unlicensed user is still owner of the following: """.format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp', proj_name, proj_num, server)

    if proj_objects['projects'] != []:
        Body = Body + '<br> {0}{0}{0} -(SUB)PROJECTS: <br> {0}{0}{0}{0} {1}'.format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp','<br> {0}{0}{0}{0} '.join(proj_objects['projects'])).format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp')
    if proj_objects['workbooks'] != []:
        Body = Body + '<br> {0}{0}{0} -WORKBOOKS: <br> {0}{0}{0}{0} {1}'.format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp','<br> {0}{0}{0}{0} '.join(proj_objects['workbooks'])).format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp')
    if proj_objects['datasources'] != []:
        Body = Body + '<br> {0}{0}{0} -DATASOURCES: <br> {0}{0}{0}{0} {1}'.format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp','<br> {0}{0}{0}{0} '.join(proj_objects['datasources'])).format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp')
        
    Body = Body + """<br><br>
{0} <b>Please make sure to change the ownership of the above items.</b> To do so: go to your project, click on the <br>
{0} 3 dots (…) next to the workbook(s) in question and choose “Change Owner” from the drop-down menu, and assign a <br>
{0} new owner (ideally an existing Publisher in your project). <br>
<br>
{0} Please also be aware that <b>when changing the owner of a workbook or data source that includes embedded credentials</b> <br>
{0} to connect to underlying data, <b>the embedded credentials will be deleted.</b> <br>
{0} The new owner can download the workbook or data source, and open the item in Tableau Desktop to update the <br>
{0} embedded credentials and then re-publish the workbook or data source. <br>
<br>
{0} Additional information on how to edit connection on Tableau server can be found <a href='https://onlinehelp.tableau.com/current/server/en-us/connections_modify.htm'>here</a>.<br>
<br>
{0} If the connection is set to “Prompt user”, then no additional action is required.<br>
<br>
{0} Thank you in advance for your cooperation!<br>
<br>
{0} Kind regards,<br>
{0} Tableau Support""".format('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp')
    
    message.HTMLBody = Body
    message.Display()

	
def main(server_config, username, password, readonly_pw, log = ''):
	
    """
    This function search for unlicensed users and if they  
    
    'username'        Tableau ECB/ESCB username (Admin)
    'password'        Tableau ECB/ESCB password (Admin)
    'server_config'   from config()
    """
    log = log + """

-------------------------------------------------------
----- Connecting to """ + server_config['postgreSQL'] + """ ------
-------------------------------------------------------

"""
    try:
        postgre_data = postgresql(readonly_pw, server_config['postgreSQL'],'select * from projects')
        postgre_unlicensed = postgresql(readonly_pw, server_config['postgreSQL'],"select u.luid, su.name, _users.licensing_role_name from users u inner join system_users su on u.system_user_id = su.id inner join _users on u.id = _users.id where _users.licensing_role_name like 'Unlicensed'")
        index = [i for i in range(len(list(postgre_unlicensed[1]))) if list(postgre_unlicensed[1])[i] is None]
        postgre_unlicensed = postgre_unlicensed.drop(postgre_unlicensed.index[index])
        df_groups = postgresql(readonly_pw, server_config['postgreSQL'],'select g.luid as "Groupid",su.email as "Username", u.luid as "Userid" from group_users gu inner join groups g on g.id=gu.group_id inner join users u on u.id=gu.user_id inner join system_users su on su.id=u.system_user_id')

    except Exception as err:
        log = log + "\n\nERROR: could not connect to the postgreSQL server, verify readonly password and retry."
        log_file(log)

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

    log = log + """

----------------------------------------------------------------------
----- signin in """ + server_config['server'] +" as " + username + """ ------
-----------------------------------------------------------------------

"""
    try:
        auth_token, site_id, user_id = sign_in(session, server, username, password)
        log = log + " ---> succeded"
    except Exception as err:
        log = log + "\n\nERROR: could not sign in server {0}".format(server)
        log_file(log)
    
    log = log + '\n connection verified \n '
    
    ### STEP 2: find users and remove unlicesed ones ###
    print("\n2. find and remove unlicensed users")
    unlicensed_users, unlius_emails, log, NoPLtext, emm = find_and_remove(session, server,auth_token,site_id,user_id, postgre_data, postgre_unlicensed, df_groups, log)
	
	##### STEP 3: Sign out #####
    print("\n3. Signing out and invalidating the authentication token")
    ##sign_out(server, auth_token)
    
    return unlicensed_users, unlius_emails, log, NoPLtext, emm
