#!/usr/bin/python
# -*- coding: utf-8 -*-

ANSIBLE_METADATA = { 'status': ['preview'],
                     'supported_by': 'community',
                     'metadata_version': '0.1',
                     'version': '0.1'}

DOCUMENTATION = '''
module: mongodb_replicaset
version_added: "2.2"
short_description: Add a shard to a MongoDB cluster

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
  replica_set_member_host:
    description:
      - Host of one member of the shard to add to MongoDB cluster
    required: True
  replica_set_member_port:
    description:
      - Port of one member of the shard to add to MongoDB cluster
    default: 27018
  replica_set:
    description:
      - Name of the ReplicaSet of the Shard
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
- name: Add shard to cluster
  mongodb_shardcluster:
    replica_set_member_host: primary.replicaset.mongo.com
    replica_set_member_port: 27018
    replica_set: myReplicaSet
'''

import ConfigParser

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

def main():
    module = AnsibleModule(
        argument_spec=dict(
            login_user=dict(default=None),
            login_password=dict(default=None, no_log=True),
            login_host=dict(default='localhost'),
            login_port=dict(default=27017, type='int'),
            replica_set_member_host=dict(default=None),
            replica_set_member_port=dict(default=27018),
            replica_set=dict(default=None, required=True),
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
        module.fail_json(msg='unable to connect to database: %s' % str(e)),

    # Check if already registred

    data = []
    try:
        current_shards = client.admin.command('listShards')
        data.append(current_shards)
        for shard in current_shards['shards']:
          if shard['_id'] == replica_set:
            module.exit_json(changed=False, msg="ReplicaSet already registred")
        
        add_shard_data = replica_set + "/" + replica_set_member_host + ":" + replica_set_member_port
        data.append(add_shard_data)
        client.admin.command('addShard', add_shard_data)
        module.exit_json(changed=True)
    except Exception as e:
        module.fail_json(msg="Error occurs adding shard: %s" % e.message, data="%s" % str(data))


from ansible.module_utils.basic import *
from ansible.module_utils.pycompat24 import get_exception

if __name__ ==  '__main__':
     main()
