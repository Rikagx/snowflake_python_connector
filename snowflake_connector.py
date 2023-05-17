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
import os

PASSWORD = os.getenv('SNOWSQL_PWD')
WAREHOUSE = os.getenv('WAREHOUSE')


db_connect(server, user, password)



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

    
# Using RSA keys to make a connection

import snowflake.connector
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
with open("<path>/rsa_key.p8", "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

ctx = snowflake.connector.connect(
    user='<user>',
    account='<account_identifier>',
    private_key=pkb,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )

cs = ctx.cursor()



