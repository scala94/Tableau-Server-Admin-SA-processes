The process identifies the Empty Projects and send an email notification to their Project Leaders asking for deletion.
In case no PL was found, Projects Leaders in parent project (if exists) are identified and notified.
Info regarding the session is stored in log.txt file

before launching:

enter the empty_projects_GUI.py file
at line 17-20 fill with Tableau Server name and connected postgreSQL
save

to launch: -> python empty_projects_GUI.py [server1] [server2] [server3] [server4]
