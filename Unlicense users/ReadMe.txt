This document describes the clean-up maintenance processes for unlicensed users on Tableau servers. 
The activity should be scheduled every month and need to be run on all Tableau Servers. 
All emails send for this task MUST contain tableau Support in CC and MUST be saved/moved in the folder “Tableau Server Unlicensed Users” in the email box.
Procedure for managing unlicensed users on Tableau Server
This section outlines the procedure for unlicensed users on the four Tableau Server instances. 


before launching:

enter the unlicensed_users_GUI.py file
at line 18-21 fill with Tableau Server name and connected postgreSQL
save

to launch: -> python unlicensed_users_GUI.py [server1] [server2] [server3] [server4]
