This python script will take CSV files and upload them to the target Azure SQL Database or Azure SQL Data Warehouse.

Requirements: 
1. Proper drivers to connect to the target SQL Database.  The current versions of Azure's SQL services require ODBC Driver 17 for SQL Server
Windows: Download and install the driver from https://www.microsoft.com/en-us/download/details.aspx?id=56567
Linux and macOS: https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017

2. An instance of Azure blob storage
3. An instance of Azure SQL Database or Azure SQL Data Warehouse.
4. An Azure Active Directory instance with an app registration with contributor permissions. 
For information on how to set this up, see https://docs.microsoft.com/en-us/azure/active-directory/develop/active-directory-integrating-applications


To use this script:

1.  Place all of your CSV files that you would like to upload into the upload/ folder.  If you would rather specify a custom upload directory, 
configure the upload_directory variable in setting.py.

2.  Configure settings.py.  
	See Settings Explanation.txt for more information.

3.	Run main.py.