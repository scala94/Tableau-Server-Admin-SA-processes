This document describes the subscription maintenance processes on Tableau servers. The activity should be scheduled every 2 weeks and need to be run on all Tableau Servers.
Procedure for managing subscriptions on Tableau Server
This section outlines the procedure for managing subscriptions on the four Tableau Server instances. 


before launching:

enter the subscriptions_failed_GUI.py file
at line 16-19 fill with Tableau Server name, connected postgreSQL and description of the server
save

to launch: -> python subscriptions_failed_GUI.py [server1] [server2] [server3] [server4]
