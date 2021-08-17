# -*- coding: utf-8 -*-
"""
Created on Mon Mar 29 19:56:46 2021

@author: scalabr
"""

import argparse
from datetime import datetime
from tkinter import *
from functools import partial
import refresh_extract_failed as ref
from datetime import datetime
import os

server_dict = {'server1': {'server':'', 'postgreSQL' : '','info':''},
                'server2': {'server':'', 'postgreSQL' : '','info':''},
                'server3': {'server':'', 'postgreSQL' : '','info':''},
                'server4': {'server':'', 'postgreSQL' : '','info':''}}

def validateLogin(ECBA_username, 
                  ESCBA_username, 
                  ECBP_username, 
                  ESCBP_username, 
                  ECBA_password, 
                  ESCBA_password, 
                  ECBP_password, 
                  ESCBP_password,
                  readonly_ECBA_pw,
                  readonly_ESCBA_pw,
                  readonly_ECBP_pw,
                  readonly_ESCBP_pw):
    
    global log
    
    if ECBA_username != None and ECBA_password != None and readonly_ECBA_pw != None:
        ECBA_user = ECBA_username.get()
        ECBA_pw = ECBA_password.get()
        read_ECBA_pw = readonly_ECBA_pw.get()
    else: 
        ECBA_user = None
        ECBA_pw = None
        read_ECBA_pw = None
        
        
    if ESCBA_username != None and ESCBA_password != None and readonly_ESCBA_pw != None:
        ESCBA_user = ESCBA_username.get()
        ESCBA_pw = ESCBA_password.get()
        read_ESCBA_pw = readonly_ESCBA_pw.get()
    else: 
        ESCBA_user = None
        ESCBA_pw = None
        read_ESCBA_pw = None
    
    if ECBP_username != None and ECBP_password != None and readonly_ECBP_pw != None:
        ECBP_user = ECBP_username.get()
        ECBP_pw = ECBP_password.get()
        read_ECBP_pw = readonly_ECBP_pw.get()
    else: 
        ECBP_user = None
        ECBP_pw = None
        read_ECBP_pw = None
        
    if ESCBP_username != None and ESCBP_password != None and readonly_ESCBP_pw != None:
        ESCBP_user = ESCBP_username.get()
        ESCBP_pw = ESCBP_password.get()
        read_ESCBP_pw = readonly_ESCBP_pw.get()
    else: 
        ESCBP_user = None
        ESCBP_pw = None
        read_ESCBP_pw = None
    
    Tab_users = [ECBA_user, ESCBA_user, ECBP_user, ESCBP_user]
    Tab_pw = [ECBA_pw, ESCBA_pw, ECBP_pw, ESCBP_pw]
    readonly_passwords = [read_ECBA_pw, read_ESCBA_pw, read_ECBP_pw, read_ESCBP_pw]
    
    log = """
##################################
# Failed Extract Regresh process #
##################################

Server selected: {0}""".format(', '.join(args.servers))
    
    tkWindow.destroy()

    for x in range(len(Tab_users)):
        if Tab_users[x] != None:
            
            if x == 0:
                server = server_dict['server1']
                readonly_pw = readonly_passwords[0]
            elif x == 1:
                server = server_dict['server2']  
                readonly_pw = readonly_passwords[1]
            elif x == 2:
                server = server_dict['server3']
                readonly_pw = readonly_passwords[2]
            else:
                server = server_dict['server4']
                readonly_pw = readonly_passwords[3]
            
            log = log + '\n\n#############{0}###############\n\n-----------connecting to postgreSQL (host {1})-----------'.format(server['info'],server['postgreSQL'])
            try:
                failed_list_final = ref.postgresql(readonly_pw, server['postgreSQL'],"select t.*, w.name  from tasks t inner join workbooks w on t.obj_id = w.id where t.type IN ('IncrementExtractTask','RefreshExtractTask')  and t.consecutive_failure_count > 4 and t.obj_type = 'Workbook' UNION select t.*, d.name  from tasks t inner join datasources d on t.obj_id = d.id where t.type IN ('IncrementExtractTask','RefreshExtractTask') and t.consecutive_failure_count > 4 and t.obj_type = 'Datasource';")
                failed_list_pre = []
                if list(failed_list_final[0])[0] == None and len(failed_list_final) == 1:
                    log = log + '\n\nNo extract refresh task failed for 5 days or more'
                    failed_list = []
                else:
                    for i in range(len(failed_list_final)):
                        info = {'object':failed_list_final.iloc[i][8], 'title':failed_list_final.iloc[i][21], 'id':failed_list_final.iloc[i][4], 'task_id':failed_list_final.iloc[i][9]}
                        failed_list_pre.append(info)

                    workbooks = ref.postgresql(readonly_pw, server['postgreSQL'],'select * from workbooks')
                    datasources =  ref.postgresql(readonly_pw, server['postgreSQL'],'select * from datasources')
                    failed_list = []

                    for fl in failed_list_pre:
                        fl['check_id'] = fl['id']
                        if fl['object'].lower() == 'workbook':
                            index = [i for i in range(len(list(workbooks[1]))) if fl['id'] == list(workbooks[0])[i]]
                        elif fl['object'].lower() == 'datasource':
                            index = [i for i in range(len(list(datasources[1]))) if fl['id'] == list(datasources[0])[i]]
                            check = datasources[(datasources[1] == fl['title'])].iloc[0]
                            if 'embedded' not in check[2]:
                                fl['id'] = check[2]
                        if len(index) == 1:
                            failed_list.append(fl)
                    df_groups = ref.postgresql(readonly_pw, server['postgreSQL'],'select g.luid as "Groupid",su.email as "Username", u.luid as "Userid" from group_users gu inner join groups g on g.id=gu.group_id inner join users u on u.id=gu.user_id inner join system_users su on su.id=u.system_user_id')
            
            except Exception as err:
                log = log + '\n\nERROR: could not connect to {0}'.format(server['info'])
                ref.log_file(log)

            if len(failed_list) != 0:
                log = log + '\n\nExtract refresh task is failing for the following objects for 5 consecutive days or more:\n{}'.format('\n'.join(['- ' + fl['title'] + ' (' + fl['object'] + ')' for fl in failed_list]))
                print(failed_list)
                log = ref.extract_refresh_delete(Tab_users[x], Tab_pw[x], server, failed_list, df_groups, log)


    log = log + '\n\nEXTRACT REFRESH PROCESS COMPLETE!'
    logfile_name = datetime.now().strftime("logs/log_%m%d_%H%M%S.txt")
    file = open(logfile_name, "w") 
    file.write(log)
    file.close()
    os.system(logfile_name.replace('/', '\\'))
    return 


if __name__ == "__main__":
    #List of servers to be processed, populated by command line arguments
    selected_servers = []

    #Command line argument parser. Positional arguments for the servers to be specified, as well as optional --all argument to process every server
    parser = argparse.ArgumentParser(description='Tableau failed extract refresh script: \
            The process identify the extract refresh tasks that failed for 5 consecutive days and uschedule them.')
    parser.add_argument('servers', metavar='server', type=str, nargs='+', choices=['ecbprod', 'ecbacc', 'iamprod', 'iamacc', 'none'], default='none',
                    help="Server(s) in scope. Please choose from 'ecbprod', 'ecbacc', 'iamprod', 'iamacc', separated by spaces.")
    args = parser.parse_args()
    
    if args.servers == 'none':
        parser.error('Please specify the servers for the failed extract refresh process by adding ecbprod, ecbacc, iamprod, iamacc or --all. For more information: tableau_housekeeping.py -h')
    else:
        for s in args.servers:
            selected_servers.append(server_dict[s])

    print("The following server will be checked: {0}".format(', '.join(args.servers)))
    
    tkWindow = Tk() 
    tkWindow.title('Failed Extract Refresh - GUI')
    
    #ECB Acceptance input details:
    #ECB Acc username label and text entry box
    if 'server1' in args.servers:
        ECBAcc_usernameLabel = Label(tkWindow, text="Server1 Admin Username").grid(row=0, column=0)
        ECBAcc_username = StringVar()
        ECBAcc_usernameEntry = Entry(tkWindow, textvariable=ECBAcc_username).grid(row=0, column=1)  
        
        #ECB Acc password label and password entry box
        ECBAcc_passwordLabel = Label(tkWindow,text="Server1 Admin Password").grid(row=1, column=0)  
        ECBAcc_password = StringVar()
        ECBAcc_passwordEntry = Entry(tkWindow, textvariable=ECBAcc_password, show='*').grid(row=1, column=1) 
        
        #ECB Acc posgreSQL readonlylabel and password entry box
        passwordServer1 = Label(tkWindow,text="Server1 readonly password").grid(row=2, column=0)  
        readonly_ECBA_pw = StringVar()
        passwordEntry1 = Entry(tkWindow, textvariable=readonly_ECBA_pw, show='*').grid(row=2, column=1)  
    else:
        ECBAcc_username = None
        ECBAcc_password = None
        readonly_ECBA_pw = None
    
    #ESCB Acceptance input details:
    #ESCB Acc username label and text entry box
    if 'server2' in args.servers:
        ESCBAcc_usernameLabel = Label(tkWindow, text="Server2 Admin Username").grid(row=3, column=0)
        ESCBAcc_username = StringVar()
        ESCBAcc_usernameEntry = Entry(tkWindow, textvariable=ESCBAcc_username).grid(row=3, column=1)  
        
        #ESCB Acc password label and password entry box
        ESCBAcc_passwordLabel = Label(tkWindow,text="Server2 Admin Password").grid(row=4, column=0)  
        ESCBAcc_password = StringVar()
        ESCBAcc_passwordEntry = Entry(tkWindow, textvariable=ESCBAcc_password, show='*').grid(row=4, column=1) 
        
        #ESCB Acc posgreSQL readonlylabel and password entry box
        passwordServer2 = Label(tkWindow,text="Server2 readonly password").grid(row=5, column=0)  
        readonly_ESCBA_pw = StringVar()
        passwordEntry2 = Entry(tkWindow, textvariable=readonly_ESCBA_pw, show='*').grid(row=5, column=1)  
    else:
        ESCBAcc_username = None
        ESCBAcc_password = None
        readonly_ESCBA_pw = None

    #ECB Production input details:
    #ECB Pro username label and text entry box
    if 'server3' in args.servers:
        ECBPro_usernameLabel = Label(tkWindow, text="Server3 Admin Username").grid(row=6, column=0)
        ECBPro_username = StringVar()
        ECBPro_usernameEntry = Entry(tkWindow, textvariable=ECBPro_username).grid(row=6, column=1)  
        
        #ECB Pro password label and password entry box
        ECBPro_passwordLabel = Label(tkWindow,text="Server3 Admin Password").grid(row=7, column=0)  
        ECBPro_password = StringVar()
        ECBPro_passwordEntry = Entry(tkWindow, textvariable=ECBPro_password, show='*').grid(row=7, column=1) 
        
        #ECB Prod posgreSQL readonlylabel and password entry box
        passwordServer3 = Label(tkWindow,text="Server3 readonly password").grid(row=8, column=0)  
        readonly_ECBP_pw = StringVar()
        passwordEntry3 = Entry(tkWindow, textvariable=readonly_ECBP_pw, show='*').grid(row=8, column=1)  
    else:
        ECBPro_username = None
        ECBPro_password = None
        readonly_ECBP_pw = None

    #ESCB Production input details:
    #ESCB Pro username label and text entry box
    if 'server4' in args.servers:
        ESCBPro_usernameLabel = Label(tkWindow, text="Server4 Admin Username").grid(row=9, column=0)
        ESCBPro_username = StringVar()
        ESCBPro_usernameEntry = Entry(tkWindow, textvariable=ESCBPro_username).grid(row=9, column=1)  
        
        #ESCB Pro password label and password entry box
        ESCBPro_passwordLabel = Label(tkWindow,text="Server4 Admin Password").grid(row=10, column=0)  
        ESCBPro_password = StringVar()
        ESCBPro_passwordEntry = Entry(tkWindow, textvariable=ESCBPro_password, show='*').grid(row=10, column=1) 
        
        #ECB Prod posgreSQL readonlylabel and password entry box
        passwordServer4 = Label(tkWindow,text="Server4 readonly password").grid(row=11, column=0)  
        readonly_ESCBP_pw = StringVar()
        passwordEntry4 = Entry(tkWindow, textvariable=readonly_ESCBP_pw, show='*').grid(row=11, column=1)  
    else:
        ESCBPro_username = None
        ESCBPro_password = None
        readonly_ESCBP_pw = None
    
    # Launch the GUI
    #validateLogin = partial(validateLogin, ECBAcc_username, ESCBAcc_username, ECBPro_username, ESCBPro_username)
    validateLogin = partial(validateLogin, 
                            ECBAcc_username, 
                            ESCBAcc_username, 
                            ECBPro_username, 
                            ESCBPro_username,  
                            ECBAcc_password, 
                            ESCBAcc_password, 
                            ECBPro_password, 
                            ESCBPro_password, 
                            readonly_ECBA_pw,
                            readonly_ESCBA_pw,
                            readonly_ECBP_pw,
                            readonly_ESCBP_pw)
    
    loginButton = Button(tkWindow, text="Check Failed Extract Refresh", command=validateLogin).grid(row=18, column=0)  
    
    tkWindow.mainloop()

    
    
    
    
    