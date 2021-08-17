This document describes the refresh extract maintenance processes on Tableau servers. 
The activity should be scheduled every 2 weeks and need to be run on all Tableau Servers.
Procedure for managing extract refreshes on Tableau Server
This section outlines the procedure for managing extract refresh failures on the four Tableau Server instances. 


before launching:

enter the refresh_extract_failed_GUI.py file
at line 16-19 fill with Tableau Server name and connected postgreSQL
save

to launch: -> python refresh_extract_failed_GUI.py [server1] [server2] [server3] [server4]
