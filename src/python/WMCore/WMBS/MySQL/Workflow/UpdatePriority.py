"""
__UpdatePriority__

MySQL implementation of UpdatePriority

Created on Apr 16, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class UpdatePriority(DBFormatter):
    """
    _UpdatePriority_

    Update the priority of the workflows given
    the name.
    """
    sql = """UPDATE wmbs_workflow SET priority = :priority
             WHERE wmbs_workflow.name = :name"""

    def execute(self, workflowName, priority,
                conn = None, transaction = False):
        self.dbi.processData(self.sql, {'name' : workflowName, 'priority' : priority},
                             conn = conn, transaction = transaction)
