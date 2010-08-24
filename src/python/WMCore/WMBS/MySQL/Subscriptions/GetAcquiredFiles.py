#!/usr/bin/env python
"""
_GetAcquiredFiles_

MySQL implementation of Subscription.GetAcquiredFiles
"""

__all__ = []
__revision__ = "$Id: GetAcquiredFiles.py,v 1.6 2009/01/16 22:42:03 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetAcquiredFiles(DBFormatter):
    sql = """SELECT file FROM wmbs_sub_files_acquired
             WHERE subscription = :subscription"""

    def formatDict(self, results):
        """
        _formatDict_

        Cast the file column to an integer as the DBFormatter's formatDict()
        method turns everything into strings.  Also, fixup the results of the
        Oracle query by renaming "fileid" to file.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        for formattedResult in formattedResults:
            if "file" in formattedResult.keys():
                formattedResult["file"] = int(formattedResult["file"])
            else:
                formattedResult["file"] = int(formattedResult["fileid"])

        return formattedResults
    
    def execute(self, subscription=None, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"subscription": subscription},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
