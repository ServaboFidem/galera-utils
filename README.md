# galera-utils
Various scripts for managing a galera cluster

# galera-restore.py
Basic script which detects if your Galera instance has crashed, automatically run wsrep recover, get the current uuid and seqno from /var/log/mysql/error.log and update /var/lib/mysql/grastate.dat with them so all you have to do is restart the node.
