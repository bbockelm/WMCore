#!/usr/bin/env python
"""
_FailFiles_

MySQL implementation of Subscription.FailFiles
"""

from WMCore.Database.DBFormatter import DBFormatter

class FailFiles(DBFormatter):
    sql = """INSERT INTO wmbs_sub_files_failed (subscription, file)
               SELECT :subscription, :fileid FROM dual WHERE NOT EXISTS
                 (SELECT file FROM wmbs_sub_files_failed
                    WHERE file = :fileid AND subscription = :subscription)"""    

    delAcq = """DELETE FROM wmbs_sub_files_acquired
                WHERE subscription = :subscription AND
                      file = :fileid"""

    delAva = """DELETE FROM wmbs_sub_files_available
                WHERE subscription = :subscription AND
                      file = :fileid"""
    
    delCom = """DELETE FROM wmbs_sub_files_complete
                WHERE subscription = :subscription AND
                      file = :fileid"""    

    def execute(self, subscription = None, file = None, conn = None,
                transaction = False):
        binds = self.getBinds(subscription = subscription, fileid = file)
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.delAcq, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.delAva, binds, conn = conn,
                             transaction = transaction)
        self.dbi.processData(self.delCom, binds, conn = conn,
                             transaction = transaction)        
        return
