# pip install snowflake-connector-Python
# pip install snowflake-sqlalchemy

## Prep work

# Create a free trial Snowflake account or use existing account
# Gather The Snowflake account url  to connect to, including the region. This URL is formed by https://<Account>.<Account_Region>.snowflakecomputing.com
# An existing user within that account
# The corresponding password for that user

import sys
import snowflake.connector as sf
from getpass2 import getpass

sfAccount = 'myAccount.my-region'
sfUser = 'my_user'

sfPswd = ''

# Request user password if not provided already    
if sfPswd == '' :
  sfPswd = getpass.getpass('Password:')

# Test the connection to Snowflake by retrieving the version number
try:
    sfConnection = sf.connect(
        user=sfUser,
        password=sfPswd,
        account=sfAccount
    )
    sfq = sfConnection.cursor()
    sfq.execute("SELECT current_version()") #executed in the Snowflake environment
    sfResults = sfq.fetchall()
    print('Snowflake Version: ' + sfResults[0][0])
    sfq.close()
    sfConnection.close()
except:
    print('Connection failed. Check credentials')


# Examples of SQL commands
sfq.execute('CREATE DATABASE DEMO_DB')
sfq.execute('USE DATABASE DEMO_DB')
sfq.execute('CREATE SCHEMA DEMO_SCHEMA')
sfq.execute('USE DEMO_DB.DEMO_SCHEMA')
sfq.execute('CREATE STAGE DEMO_STAGE')

# Example of function to create db and schema
def CreateSnowflakeDBandSchema (
    sfPswd = '',
    sfAccount = 'myAccount.my-region',
    sfUser = 'my_user',
    sfDatabase = 'DEMO_DB',
    sfSchema = 'DEMO_SCHEMA'
):
    import snowflake.connector as sf

    # Request user password if not provided already
    if sfPswd == '' :
      import getpass
      sfPswd = getpass.getpass('Password:')

    # Test the connection to Snowflake
    try:
      sfConnection = sf.connect(
          user=sfUser,
          password=sfPswd,
          account=sfAccount
      )
      sfq = sfConnection.cursor()
      # sfq.execute("SELECT current_version()")
      # sfResults = sfq.fetchall()
      # print('Snowflake Version: ' + sfResults[0][0])
      sfq.close()
      sfConnection.close()
    except:
      print('Connection failed. Check credentials')

    # Open connection to Snowflake
    sfConnection = sf.connect(
      user=sfUser,
      password=sfPswd,
      account=sfAccount
    )

    sfq = sfConnection.cursor()

    sfq.execute('CREATE DATABASE IF NOT EXISTS {0}'.format(sfDatabase))
    sfq.execute('CREATE SCHEMA IF NOT EXISTS {0}.{1}'.format(sfDatabase, sfSchema))

    print('Steps complete')

    




