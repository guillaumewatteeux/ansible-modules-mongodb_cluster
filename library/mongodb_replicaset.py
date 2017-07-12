#!/usr/bin/python
# -*- coding: utf-8 -*-

ANSIBLE_METADATA = { 'status': ['preview'],
                     'supported_by': 'community',
                     'metadata_version': '0.1',
                     'version': '0.1'}  

DOCUMENTATION = '''
module: mongodb_replicaset
version_added: "2.2"
short_description: configure a MongoDB ReplicaSet

options:
  login_user:
    description:
      - The username used to authenticate with
    required: False
    default: None
  login_password:
    description:
      - The password used to authenticate with
    required: False
    default: None
  login_host:
    description:
      - The host running the database
    required: False
    default: localhost
  login_port:
    description:
      - The port to connect to
    required: False
    default: 27017
  command:
    description:
      - command to execute. Only 'get_primary' command return values. See Return Value section.
    default: get_primary
    choices: ['initiate','add_member', 'get_primary']
  replica_set_member_host:
    description:
      - Host of the member to add to ReplicaSet
    required: True
  replica_set_member_port:
    description:
      - Port of the member to add to ReplicaSet
    default: 27018
  replica_set:
    description:
      - Name of the ReplicaSet
    required: True
  ssl:
    description:
      - Whether to use an SSL connection when connecting to the database
    required: False
    default: False

notes:
    - Requires the pymongo Python package on the remote host, version 2.4.2+. This
      can be installed using pip or the OS package manager. @see http://api.mongodb.org/python/current/installation.html
requirements: [ "pymongo" ]
'''

EXAMPLES = '''
- name: Create replicaSet (initiate)
  mongodb_replicaset:
    command: initiate
    login_port: 27018
    replica_set_member_host: primary.replicaset.mongo.com
    replica_set_member_port: "27018"
    replica_set: myReplicaSet

- name: Get primary
  mongodb_replicaset:
    command: get_primary
    login_port: 27018
    replica_set: myReplicaSet

- name: Add member
  mongodb_replicaset:
    command: add_member
    login_port: 27018
    replica_set_member_host: secondary1.replicaset.mongo.com
    replica_set_member_port: 27018
    replica_set: myReplicaSet
'''

RETURN = '''
server:
  description: host:port of the primary
  returned: host:port
  type: string
  sample: "primary.replicaset.mongo.com:27018"
host:
  description: host  of the primary
  returned: host
  type: string
  sample: "primary.replicaset.mongo.com"

'''


import ConfigParser
import time

try:
    from pymongo.errors import ConnectionFailure
    from pymongo.errors import OperationFailure
    from pymongo import version as PyMongoVersion
    from pymongo import MongoClient
except ImportError:
    try:  # for older PyMongo 2.2
        from pymongo import Connection as MongoClient
    except ImportError:
        pymongo_found = False
    else:
        pymongo_found = True
else:
    pymongo_found = True


# =========================================
# MongoDB module specific support methods.
#

def load_mongocnf():
    config = ConfigParser.RawConfigParser()
    mongocnf = os.path.expanduser('~/.mongodb.cnf')

    try:
        config.readfp(open(mongocnf))
        creds = dict(
            user=config.get('client', 'user'),
            password=config.get('client', 'pass')
        )
    except (ConfigParser.NoOptionError, IOError):
        return False

    return creds

def get_primary(admin_client):
    replStatus = admin_client.command('replSetGetStatus')
    for member in replStatus['members']:
        if member['stateStr'] == 'PRIMARY':
            return member['name']
    return None


# =========================================
# Module execution.
#

def main():
    module = AnsibleModule(
        argument_spec=dict(
            login_user=dict(default=None),
            login_password=dict(default=None, no_log=True),
            login_host=dict(default='localhost'),
            login_port=dict(default=27017, type='int'),
            command=dict(default='get_primary', choices=['initiate','add_member', 'get_primary']),
            replica_set_member_host=dict(required=True),
            replica_set_member_port=dict(default=27018),
            replica_set=dict(required=True),
            ssl=dict(default=False, type='bool'),
        )
    )

    if not pymongo_found:
        module.fail_json(msg='the python pymongo module is required')

    login_user = module.params['login_user']
    login_password = module.params['login_password']
    login_host = module.params['login_host']
    login_port = module.params['login_port']

    replica_set_member_host = module.params['replica_set_member_host']
    replica_set_member_port = module.params['replica_set_member_port']

    replica_set = module.params['replica_set']
    ssl = module.params['ssl']

    command = module.params['command']

    try:
        client = MongoClient(login_host, int(login_port), ssl=ssl)

        if login_user is None and login_password is None:
            mongocnf_creds = load_mongocnf()
            if mongocnf_creds is not False:
                login_user = mongocnf_creds['user']
                login_password = mongocnf_creds['password']
        elif login_password is None or login_user is None:
            module.fail_json(msg='when supplying login arguments, both login_user and login_password must be provided')

        if login_user is not None and login_password is not None:
                client.admin.authenticate(login_user, login_password)

    except ConnectionFailure:
        e = get_exception()
        module.fail_json(msg='unable to connect to database: %s' % str(e))
    
    if command == 'initiate':
        try:
            replicaSetConfig = client.admin.command('replSetGetStatus') # Throw an OperationFailure if no replicaset
            if replicaSetConfig['set'] == replica_set:
              module.exit_json(changed=False, msg="A replicaSet already exist on this host " + replica_set_member_host + ":" + replica_set_member_port)
            else: 
                module.fail_json(msg="Unable to initiate replicaSetr: ReplicaSet" + replicaSetConfig['set'] + "is already defined on this host")
            
        except OperationFailure:
            e = get_exception()
            initiateCommandDoc = {
                'replSetInitiate':{
                    '_id': replica_set,
                    'members': [
                        {'_id': 0, 'host': replica_set_member_host + ":" + replica_set_member_port}
                    ]
                }
            }
            client.admin.command(initiateCommandDoc)
            maxtimeout = 10
            timeout = 0
            primary = get_primary(client.admin)

            while primary is None and timeout < maxtimeout:
                time.sleep(1)
                primary = get_primary(client.admin)
                timeout+=1
            module.exit_json(changed=True, msg="Created replicaSet" + replica_set)
      
    elif command == 'get_primary':
        try:
            primary = get_primary(client.admin)
            if primary is None:
                module.fail_json(msg="Unable to find PRIMARY member")
            else:
                module.exit_json(changed=False, server=primary, host=primary.split(':')[0])
        except Exception as e:
          module.fail_json(msg="Unable to get primary informations: %s" % e.message)
    
    elif command == 'add_member':
        data = []
        try:
            data = []
            config = client.admin.command('replSetGetConfig')['config']
            data.append(config)
            ids = []
            for member in config['members']:
                cmp_host = str(replica_set_member_host + ":" + replica_set_member_port)
                if member['host'] == cmp_host:
                    module.exit_json(change=False)
                ids.append(member['_id'])

            config['version'] += 1
            config['members'].append({'_id': max(ids)+1, 'host': replica_set_member_host + ":" + replica_set_member_port})
            client.admin.command('replSetReconfig', config)
            module.exit_json(changed=True)
        except Exception as e:
            module.fail_json(msg="Unable to get primary informations: %s" % e.message, data="%s" % str(data))


# import module snippets
from ansible.module_utils.basic import *
from ansible.module_utils.pycompat24 import get_exception


if __name__ ==  '__main__':
     main()
