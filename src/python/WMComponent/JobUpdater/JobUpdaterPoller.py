"""
__JobUpdaterPoller__

Poller module for the JobUpdater, takes care of the
actual updating work.

Created on Apr 16, 2013

@author: dballest
"""

import logging
import threading

from WMCore.BossAir.BossAirAPI import BossAirAPI
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMException import WMException

class JobUpdaterException(WMException):
    """
    _JobUpdaterException_

    A job updater exception-handling class for the JobUpdaterPoller
    """
    pass

class JobUpdaterPoller(BaseWorkerThread):
    """
    _JobUpdaterPoller_

    Poller class for the JobUpdater
    """

    def __init__(self, config):
        """
        __init__
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        self.bossAir = BossAirAPI(config = self.config)
        self.reqmgr = RequestManager({'endpoint' : self.config.JobUpdater.reqMgrUrl})
        self.workqueue = WorkQueue(self.config.WorkQueueManager.couchurl,
                                   self.config.WorkQueueManager.dbname)

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        self.listWorkflowsDAO = self.daoFactory(classname = "Workflow.ListForJobUpdater")
        self.updateWorkflowPrioDAO = self.daoFactory(classname = "Workflow.UpdatePriority")


    def setup(self, parameters = None):
        """
        _setup_
        """
        pass

    def terminate(self, parameters = None):
        """
        _terminate_

        Terminate gracefully.
        """
        pass
    
    def algorithm(self, parameters = None):
        """
        _algorithm_
        """
        logging.info("Synchronizing priorities with ReqMgr...")
        self.synchronizeJobPriority()

    def synchronizeJobPriority(self):
        """
        _synchronizeJobPriority_

        Check WMBS for active workflows and compare with the
        ReqMgr for priority changes. If a priority change occurs
        then update the job priority in the batch system and
        the elements in the local queue that have not been injected yet.
        """
        workflowsToCheck = self.listWorkflowsDAO.execute()
        for workflowEntry in workflowsToCheck:
            requestInfo = self.reqmgr.getRequest(workflowEntry['name'])
            requestPriority = requestInfo['RequestPriority']
            if requestPriority != workflowEntry['workflow_priority']:
                # Then we have an update to make
                self._updatePriority(workflowEntry, requestPriority)
        return

    def _updatePriority(self, workflowInfo, newPrio):
        """
        __updatePriority_

        Update the priority of the workflow in WMBS,
        the batch system and the local queue.
        """
        currentThread = threading.currentThread()
        currentThread.transaction.begin()
        try:
            # Update WMBS with the correct priority
            self.updateWorkflowPrioDAO.execute(workflowName = workflowInfo['name'],
                                               priority = newPrio,
                                               conn = currentThread.transaction.conn,
                                               transaction = currentThread.transaction)

            # Update the available elements in the local WorkQueue
            self.workqueue.updatePriority(workflowInfo['name'], newPrio)

            # Update the jobs already running in WMBS
            self.bossAir.updateJobInformation(workflowInfo['name'], workflowInfo['task'],
                                              requestPriority = newPrio,
                                              taskPriority = workflowInfo['task_priority'])

        except Exception, ex:
            if getattr(currentThread, 'transaction', None) is not None:
                currentThread.transaction.rollbackForError()
            msg = "Error trying to update the priority of workflow %s\n" % str(workflowInfo)
            msg += "Error details: %s" % str(ex)
            logging.error(msg)
        currentThread.transaction.commit()
        return