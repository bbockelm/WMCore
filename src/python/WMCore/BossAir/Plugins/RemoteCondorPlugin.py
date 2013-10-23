"""
WMCore.BossAir.Plugins.RemoteCondor - 
    Hooks up to rcondor for local glidein testing

Created by Andrew Melo <andrew.melo@gmail.com> on Oct 9, 2012
"""

import os
import re
import time
import Queue
import os.path
import logging
import hashlib
import commands
import threading
import traceback
import subprocess
import multiprocessing
import glob

import WMCore.Algorithms.BasicAlgos as BasicAlgos

from WMCore.Credential.Proxy           import Proxy
from WMCore.DAOFactory                 import DAOFactory
from WMCore.WMException                import WMException
from WMCore.WMInit                     import getWMBASE
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin, BossAirPluginException
from WMCore.FwkJobReport.Report        import Report
from WMCore.Algorithms                 import SubprocessAlgos
from WMCore.Services.SiteDB            import SiteDB

def submitWorker(input, results, timeout = None):
    """
    _outputWorker_

    Runs a subprocessed command.

    This takes whatever you send it (a single ID)
    executes the command
    and then returns the stdout result

    I planned this to do a glite-job-output command
    in massive parallel, possibly using the bulkID
    instead of the gridID.  Either way, all you have
    to change is the command here, and what is send in
    in the complete() function.
    """

    # Get this started
    while True:
        try:
            work = input.get()
        except (EOFError, IOError), ex:
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            crashMessage += str(ex)
            logging.error(crashMessage)
            break
        except Exception, ex:
            msg =  "Hit unidentified exception getting work\n"
            msg += str(ex)
            msg += "Assuming everything's totally hosed.  Killing process.\n"
            logging.error(msg)
            break

        if work == 'STOP':
            # Put the brakes on
            logging.info("submitWorker multiprocess issued STOP command!")
            break

        command = work.get('command', None)
        idList  = work.get('idList', [])
        if not command:
            results.put({'stdout': '', 'stderr': '999100\n Got no command!', 'idList': idList, 'command' : command})
            continue

        try:
            stdout, stderr, returnCode = SubprocessAlgos.runCommand(cmd = command, shell = True, timeout = timeout)
            if returnCode == 0:
                results.put({'stdout': stdout, 'stderr': stderr, 'idList': idList, 'exitCode': returnCode, 'command' : command})
            else:
                results.put({'stdout': stdout,
                             'stderr': 'Non-zero exit code: %s\n stderr: %s' % (returnCode, stderr),
                             'exitCode': returnCode,
                             'idList': idList,
                             'command' : command})
        except Exception, ex:
            msg =  "Critical error in subprocess while submitting to condor"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
            results.put({'stdout': '', 'stderr': '999101\n %s' % msg, 'idList': idList, 'exitCode': 999101, 'command' : command})

    return 0

def parseError(error):
    """
    Do some basic condor error parsing

    """

    errorCondition = True
    errorMsg       = error

    if 'ERROR: proxy has expired\n' in error:
        errorCondition = True
        errorMsg = 'CRITICAL ERROR: Your proxy has expired!\n'
    elif '999100\n' in error:
        errorCondition = True
        errorMsg = "CRITICAL ERROR: Failed to build submit command!\n"
    elif 'Failed to open command file' in error:
        errorCondition = True
        errorMsg = "CONDOR ERROR: jdl file not found by submitted jobs!\n"
    elif 'It appears that the value of pthread_mutex_init' in error:
        # glexec insists on spitting out to stderr
        lines = error.split('\n')
        if len(lines) == 2 and not lines[1]:
            errorCondition = False
            errorMsg = error

    return errorCondition, errorMsg

class RemoteCondorPlugin(BasePlugin):
    """
    _RemoteCondorPlugin_

    Remote Condor plugin for glide-in submissions to the 
    UCSD analysis glidein server
    """

    @staticmethod
    def stateMap():
        """
        For a given name, return a global state


        """

        stateDict = {'New': 'Pending',
                     'Idle': 'Pending',
                     'Running': 'Running',
                     'Held': 'Error',
                     'Complete': 'Complete',
                     'Error': 'Error',
                     'Timeout': 'Error',
                     'Removed': 'Running',
                     'Unknown': 'Error'}

        # This call is optional but needs to for testing
        #BasePlugin.verifyState(stateDict)

        return stateDict

    def __init__(self, config, **kwargs):

        self.config = config

        BasePlugin.__init__(self, config)

        self.locationDict = {}

        myThread = threading.currentThread()
        if 'logger' in kwargs:
            logger = kwargs['logger']
        else:
            logger = myThread.logger
        if hasattr(myThread, 'dbi'):
            dbi = myThread.dbi
        else:
            dbi = None
        if dbi:
            daoFactory = DAOFactory(package="WMCore.WMBS", logger = logger,
                                    dbinterface = dbi)
            self.locationAction = daoFactory(classname = "Locations.GetSiteInfo")
        else:
            self.locationAction = None


        self.packageDir = None

        if os.path.exists(os.path.join(getWMBASE(),
                                       'src/python/WMCore/WMRuntime/Unpacker.py')):
            self.unpacker = os.path.join(getWMBASE(),
                                         'src/python/WMCore/WMRuntime/Unpacker.py')
        else:
            self.unpacker = os.path.join(getWMBASE(),
                                         'WMCore/WMRuntime/Unpacker.py')

        config.section_("Agent")
        self.agent         = getattr(config.Agent, 'agentName', 'WMAgent')
        self.sandbox       = None
        self.scriptFile    = None
        self.submitDir     = None
        self.removeTime    = getattr(config.BossAir, 'removeTime', 60)
        self.multiTasks    = getattr(config.BossAir, 'multicoreTaskTypes', [])
        self.overflowTasks = getattr(config.BossAir, 'allowedOverflowTaskTypes', [])
        self.useGSite      = getattr(config.BossAir, 'useGLIDEINSites', False)
        self.submitWMSMode = getattr(config.BossAir, 'submitWMSMode', False)
        self.errorThreshold= getattr(config.BossAir, 'submitErrorThreshold', 10)
        self.errorCount    = 0


        # Build ourselves a pool
        self.pool     = []
        self.input    = None
        self.result   = None
        self.nProcess = getattr(self.config.BossAir, 'nCondorProcesses', 1)
        
        # Get a sitedb instance to convert to SE names
        self.siteDB = SiteDB.SiteDBJSON()

        # Set up my proxy and glexec stuff
        self.setupScript = getattr(config.BossAir, 'UISetupScript', None)
        self.proxy       = None
        self.serverCert  = getattr(config.BossAir, 'delegatedServerCert', None)
        self.serverKey   = getattr(config.BossAir, 'delegatedServerKey', None)
        self.myproxySrv  = getattr(config.BossAir, 'myproxyServer', None)
        self.proxyDir    = getattr(config.BossAir, 'proxyDir', '/tmp/')
        self.serverHash  = getattr(config.BossAir, 'delegatedServerHash', None)
        self.glexecPath  = getattr(config.BossAir, 'glexecPath', None)
        self.glexecWrapScript = getattr(config.BossAir, 'glexecWrapScript', None)
        self.glexecUnwrapScript = getattr(config.BossAir, 'glexecUnwrapScript', None)
        self.gsisshOptions = ["-o", "ForwardX11=no","-o", "ProxyCommand=none", "-o", "ControlPath=none"]
        #self.remoteUserHost = "se2.accre.vanderbilt.edu"
        self.remoteUserHost  = getattr(config.BossAir, 'remoteUserHost', 'submit-2.t2.ucsd.edu')
        self.jdlProxyFile    = None # Proxy name to put in JDL (owned by submit user)
        self.glexecProxyFile = None # Copy of same file owned by submit user

        if self.glexecPath:
            if not (self.myproxySrv and self.proxyDir):
                raise WMException('glexec requires myproxyServer and proxyDir to be set.')
        if self.myproxySrv:
            if not (self.serverCert and self.serverKey):
                raise WMException('MyProxy server requires serverCert and serverKey to be set.')

        # Make the directory for the proxies
        if self.proxyDir and not os.path.exists(self.proxyDir):
            logging.debug("proxyDir not found: creating it.")
            try:
                os.makedirs(self.proxyDir, 01777)
            except Exception, ex:
                msg = "Error: problem when creating proxyDir directory - '%s'" % str(ex)
                raise BossAirPluginException(msg)
        elif not os.path.isdir(self.proxyDir):
            msg = "Error: proxyDir '%s' is not a directory" % self.proxyDir
            raise BossAirPluginException(msg)

        if self.serverCert and self.serverKey and self.myproxySrv:
            self.proxy = self.setupMyProxy()

        # Build a request string
        self.reqStr = "(Memory >= 1 && OpSys == \"LINUX\" ) && (Arch == \"INTEL\" || Arch == \"X86_64\")"
        if hasattr(config.BossAir, 'condorRequirementsString'):
            self.reqStr = config.BossAir.condorRequirementsString

        # for testing
        #  this should be gsissh or gsiscp
        self.ssh = 'gsissh'
        self.scp = 'gsiscp'
        self.remoteSubdir = "bossair"


    def __del__(self):
        """
        __del__

        Trigger a close of connections if necessary
        """
        self.close()

    def setupMyProxy(self):
        """
        _setupMyProxy_

        Setup a WMCore.Credential.Proxy object with which to retrieve
        proxies from myproxy using the server Cert
        """

        args = {}
        if self.setupScript:
            args['uisource'] = self.setupScript
        args['server_cert'] = self.serverCert
        args['server_key']  = self.serverKey
        args['myProxySvr']  = self.myproxySrv
        args['credServerPath'] = self.proxyDir
        args['logger'] = logging
        return Proxy(args = args)

    def close(self):
        """
        _close_

        Kill all connections and terminate
        """
        terminate = False
        for _ in self.pool:
            try:
                self.input.put('STOP')
            except Exception, ex:
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                logging.error(msg)
                terminate = True
        try:
            self.input.close()
            self.result.close()
        except:
            # There's really not much we can do about this
            pass
        for proc in self.pool:
            if terminate:
                try:
                    proc.terminate()
                except Exception, ex:
                    logging.error("Failure while attempting to terminate process")
                    logging.error(str(ex))
                    continue
            else:
                try:
                    proc.join()
                except Exception, ex:
                    try:
                        proc.terminate()
                    except Exception, ex2:
                        logging.error("Failure to join or terminate process")
                        logging.error(str(ex))
                        logging.error(str(ex2))
                        continue
        # At the end, clean the pool and the queues
        self.pool   = []
        self.input  = None
        self.result = None
        return

    def checkAndStartPool(self, timeout):
        if len(self.pool) == 0:
            # Starting things up
            # This is obviously a submit API
            logging.info("Starting up RemoteCondorPlugin worker pool")
            self.input    = multiprocessing.Queue()
            self.result   = multiprocessing.Queue()
            for _ in range(self.nProcess):
                p = multiprocessing.Process(target = submitWorker,
                                            args = (self.input, self.result, timeout))
                p.start()
                self.pool.append(p)
    
    def getSubmissionDict(self, jobs): 
        # Now assume that what we get is the following; a mostly
        # unordered list of jobs with random sandboxes.
        # We intend to sort them by userDN and sandbox.

        submitDict = {'nodn': {}}
        for job in jobs:
            userdn = job.get('userdn', 'nodn')
            if not userdn in submitDict.keys():
                submitDict[userdn] = {}
                
            sandbox = job['sandbox']
            if not sandbox in submitDict[userdn].keys():
                submitDict[userdn][sandbox] = []
            submitDict[userdn][sandbox].append(job)
            
        return submitDict

    def getWrapEnv(self):
        wrapper = ""
        if self.glexecPath:
            if self.glexecWrapScript:
                wrapper += 'export GLEXEC_ENV=`%s 2>/dev/null`; ' % self.glexecWrapScript
            wrapper += 'export GLEXEC_CLIENT_CERT=%s; ' % self.glexecProxyFile
            wrapper += 'export GLEXEC_SOURCE_PROXY=%s; ' % self.glexecProxyFile
            wrapper += 'export X509_USER_PROXY=%s; ' % self.glexecProxyFile
            wrapper += 'export GLEXEC_TARGET_PROXY=%s; ' % self.jdlProxyFile
        return wrapper

    def getCommandWrapper(self):
        wrapper = ""
        if self.glexecPath:
            if self.glexecUnwrapScript:
                wrapper += '%s %s -- ' % (self.glexecPath, self.glexecUnwrapScript)
            else:
                wrapper += '%s ' % (self.glexecPath, )
        else:
            wrapper = ' '        
        return wrapper
    
    def makeMkdirCommand(self, targetDirs):
        wrapper = self.getCommandWrapper()
        
        wrapssh = "%s %s %s" % (wrapper, self.ssh, " ".join(self.gsisshOptions))

        # make sure there's a condor work directory on remote host
        command = "%s %s " % (wrapssh, self.remoteUserHost)
        command += " mkdir -p %s" % (" ".join(targetDirs))
        return command

    def makeRenameCommand(self, targetFiles, postfix = '-temp'):
        wrapper = self.getCommandWrapper()

        wrapssh = "%s %s %s" % (wrapper, self.ssh, " ".join(self.gsisshOptions))

        # make sure there's a condor work directory on remote host
        command = "%s %s '" % (wrapssh, self.remoteUserHost)
        for file in targetFiles:
            command += " rename %s%s %s %s%s || true;" % (file[1], postfix, file[1], file[1], postfix)
        command += "'"
        return command
    
    def makeScpOutwardCommand(self, targetFiles, postfix = '-temp'):
        wrapper = self.getCommandWrapper()
        
        wrapscp = "%s %s %s" % (wrapper, self.scp, " ".join(self.gsisshOptions))
        commands = []
        for onefile in targetFiles:
            if not os.path.exists( onefile[0] ):
                raise BossAirPluginException("Missing file to scp: %s" % onefile[0])
            commands.append('%s %s %s:%s%s' % \
                            (wrapscp, onefile[0], self.remoteUserHost, onefile[1], postfix))

        return ' && '.join(commands)
    
    def makeScpInwardCommand(self, targetFiles):
        wrapper = self.getCommandWrapper()
        
        wrapscp = "%s %s %s" % (wrapper, self.scp, " ".join(self.gsisshOptions))
        commands = []
        for onefile in targetFiles:
            commands.append('%s %s:%s %s || true' % \
                            (wrapscp, self.remoteUserHost, onefile[0], onefile[1]))

        return ' && '.join(commands)    
    
    def makeSubmitCommand(self, remoteJDL, initialDir=None):
        wrapper = self.getCommandWrapper()
        
        wrapssh = "%s %s %s" % (wrapper, self.ssh, " ".join(self.gsisshOptions))
        command = "%s %s '" % (wrapssh, self.remoteUserHost)
        if initialDir:
            command += "cd %s &&" % initialDir
        command += "condor_submit %s'" % remoteJDL

        return command
    
    def enqueueCommand(self, command, idList):
        try:
            self.input.put({'command': command, 'idList': idList})
        except AssertionError, ex:
            msg =  "Critical error: input pipeline probably closed.\n"
            msg += str(ex)
            msg += "Error Procedure: Something critical has happened in the worker process\n"
            msg += "We will now proceed to pull all useful data from the queue (if it exists)\n"
            msg += "Then refresh the worker pool\n"
            logging.error(msg)
            return False
        except Exception, ex:
            msg += str(ex)
            msg += "Error Procedure: Something critical has happened in the worker process\n"
            msg += "We will now proceed to pull all useful data from the queue (if it exists)\n"
            msg += "Then refresh the worker pool\n"
            logging.error(msg)
            return False            
        return True

    def collapseFilesForJobs(self, jobs, inputPrefix):
        """
        collapseFilesForJobs - 
            makes a list of directories, a dict of local->remote file mappings
            
            to minimize transferring the same files for every job, we make
            a remote directory structure like:
            
            bossair/<last 3 chars of request time>/<request time>/input/<md5 hash>/<name of file>
            
            That directory structure is passed to condor with transfer_input_files
            self.jdlProxyFile.
                        
            jdl.append("transfer_input_files = %s, %s/%s, %s\n" \
              % (job['sandbox'], job['packageDir'],
                 'JobPackage.pkl', self.unpacker))
        """

        requiredDirs    = [inputPrefix]
        requiredFiles   = []
        localToRemote   = {}
        localToHash     = {}
        hashToRemote    = {}
        
        for job in jobs:
            job['remoteFiles'] = []
            for onefile in (job['sandbox'],
                         "%s/%s" % (job['packageDir'], 'JobPackage.pkl'),
                         self.unpacker):
                
                if not onefile.startswith('/'):
                    onefile = job['cacheDir'] + '/' + onefile
                    
                if onefile not in localToHash:
                    hasher = hashlib.sha256()
                    hasher.update( open(onefile, 'r').read() )
                    localToHash[onefile] = hasher.hexdigest()
                
                if localToHash[onefile] not in hashToRemote:
                    remoteDir  = "%s/%s/%s/%s" % (inputPrefix, localToHash[onefile][0:2], localToHash[onefile][2:4], localToHash[onefile])
                    remoteFile = "%s/%s" % (remoteDir, os.path.basename(onefile))
                    requiredDirs.append( remoteDir )
                    requiredFiles.append( (onefile, remoteFile ) )
                    hashToRemote[localToHash[onefile]] = remoteFile
                    
                if onefile not in localToRemote:
                    localToRemote[onefile] = hashToRemote[localToHash[onefile]]
                    
                job['remoteFiles'].append( localToRemote[onefile] )
        
        requiredDirs.sort()
        return requiredDirs, requiredFiles, inputPrefix
        
    def submit(self, jobs, info):
        """
        _submit_


        Submit jobs for one subscription
        """
        if len(jobs) == 0:
            # Then was have nothing to do
            return [], []

        # If we're here, then we have submitter components. Initialize our working lists
        self.scriptFile = self.config.JobSubmitter.submitScript
        self.submitDir  = self.config.JobSubmitter.submitDir
        timeout         = getattr(self.config.JobSubmitter, 'getTimeout', 400)
        jdlFiles        = []        
        submitDict      = self.getSubmissionDict( jobs )
        nSubmits        = 0
        
        if not os.path.exists(self.submitDir):
            os.makedirs(self.submitDir)

        self.checkAndStartPool( timeout )

        # Now submit the bastards, but do it per-DN and per-sandbox
        queueError = False
        for userdn in submitDict.keys():
            if queueError:
                break
            for sandbox in submitDict[userdn].keys():
                if queueError:
                    break
                jobList = submitDict[userdn].get(sandbox, [])
                while len(jobList) > 0 and not queueError:
                    submitted, jdlTemp, queueError, jobList = self.submitJobs(jobList)
                    nSubmits += submitted
                    if jdlTemp:
                        jdlFiles.append( jdlTemp )
        
        # now check the status of the submissions
        successfulJobs, failedJobs = \
                self.checkCondorResponse( jobs, nSubmits, timeout )
        
        # Remove JDL files unless commanded otherwise
        if getattr(self.config.JobSubmitter, 'deleteJDLFiles', True):
            for f in jdlFiles:
                os.remove(f)

        # When we're finished, clean up the queue workers in order
        # to free up memory (in the midst of the process, the forked
        # memory space shouldn't be touched, so it should still be
        # shared, but after this point any action by the Submitter will
        # result in memory duplication).
        logging.info("Purging worker pool to clean up memory")
        self.close()


        # We must return a list of jobs successfully submitted,
        # and a list of jobs failed
        logging.info("Done submitting jobs for this cycle in CondorPlugin")
        return successfulJobs, failedJobs
       
    def submitJobs(self, jobList):
        jdlFiles    = []
        queueError  = False
        jobsReady   = jobList[:self.config.JobSubmitter.jobsPerWorker]
        jobList     = jobList[self.config.JobSubmitter.jobsPerWorker:]
        idList      = [x['id'] for x in jobsReady]
        
        # make a target prefix
        targetPrefix    = self.remoteSubdir
        inputPrefix     = '%s/input' % (targetPrefix)
        
        # combine exactly the same files so they get scpd less times
        reqDirs, reqFiles, inputPrefix = self.collapseFilesForJobs(jobsReady, inputPrefix)

        # make sure the JDL and submit makes it
        remoteSub = "%s/%s" % (inputPrefix, \
                                 os.path.basename(self.scriptFile))
        
        jdlList, outDirs = self.makeJDL(jobList = jobsReady, \
                                   remoteSubmitScript = remoteSub,
                                   baseOutput = self.remoteSubdir)
        
        # if we got a proxy, remember to move it
        proxiesMoved = {}
        for index in range(len(jdlList)):
            line = jdlList[index]
            if line.startswith('x509userproxy'):
                proxyFile = line.split(' = ')[1].rstrip()
                remoteProxy = "%s/%s" % (self.remoteSubdir, 'proxy.cert')
                jdlList[index] = 'x509userproxy = %s\n' % remoteProxy
                if not proxyFile in proxiesMoved:
                    reqFiles.append( (proxyFile, remoteProxy) )
                    proxiesMoved[proxyFile] = True

        reqDirs.extend( outDirs )
        # make a JDL
        jdlFile = "%s/submit_%i_%i_%i.jdl" % \
                    (self.submitDir, os.getpid(), idList[0], time.time())
        handle = open(jdlFile, 'w')
        handle.writelines(jdlList)
        handle.close()
        jdlFiles.append(jdlFile)       
        remoteJDL = "%s/%s" % (inputPrefix, os.path.basename(jdlFile))
        reqFiles.append( ( jdlFile, remoteJDL ) )
        reqFiles.append( ( self.scriptFile, remoteSub ))

        if not jdlList or jdlList == []:
            # Then we got nothing
            logging.error("No JDL file made!")
            nsubmit = 0
            return nsubmit, None, False, []

        # Now submit them
        logging.info("About to submit %i jobs" %(len(jobsReady)))
        mkdirCommand  = self.makeMkdirCommand( reqDirs )
        copyCommand   = self.makeScpOutwardCommand( reqFiles )
        submitCommand = self.makeSubmitCommand(remoteJDL)
        renameCommand = self.makeRenameCommand(reqFiles)
        totalCommand = "%s && %s && %s ; %s" % (mkdirCommand, copyCommand, renameCommand, submitCommand)
        
        if not self.enqueueCommand(totalCommand, idList):
            queueError = True
        
        nsubmit = 1
        return nsubmit, jdlFile, queueError, jobList

    def submitRaw(self, randomID, jdl, proxyFile, fileList):
        # if you're like Brian and you want to make the JDLs yourself
        # doesn't use the pool since we're at most submitting a job at a time 
        mkdirCommand = self.makeMkdirCommand([randomID])
        filesToMove = [[jdl, "%s/%s" % (randomID, 'submit.jdl')]]
        filesToMove.append([proxyFile, "%s/user.proxy" % randomID])
        for onefile in fileList:
            filesToMove.append([onefile, "%s/%s" % (randomID, os.path.basename(onefile))])
        scpCommand = self.makeScpOutwardCommand(filesToMove, '')
        subCommand = self.makeSubmitCommand('submit.jdl', randomID)
        totalCommand = "export X509_USER_PROXY=%s ;" % proxyFile
        totalCommand += " && ".join([mkdirCommand, scpCommand, subCommand])
        proc = subprocess.Popen(totalCommand, stderr = subprocess.PIPE,
                                    stdout = subprocess.PIPE, shell = True)
        stdout, stderr = proc.communicate()
        if not proc.returncode == 0:
            # Then things have gotten bad - condor_rm is not responding
            logging.error("failed to submit job: returned non-zero value %s" % str(proc.returncode))
            logging.error("command")
            logging.error(totalCommand)
            logging.error("stdout")
            logging.error(stdout)
            logging.error("stderr")
            logging.error(stderr)
            raise RuntimeError, "Failed to submit\nStdout: %s\nStderr: %s" % (stdout, stderr)

    
    def checkCondorResponse(self, jobs, nSubmits, timeout):
        failedJobs     = []
        successfulJobs = []
        
        # Now we should have sent all jobs to be submitted
        # Check their status now
        for _ in range(nSubmits):
            try:
                res = self.result.get(block = True, timeout = timeout)
            except Queue.Empty:
                # If the queue was empty go to the next submit
                # Those jobs have vanished
                logging.error("Queue.Empty error received!")
                logging.error("This could indicate a critical condor error!")
                logging.error("However, no information of any use was obtained due to process failure.")
                logging.error("Either process failed, or process timed out after %s seconds." % timeout)
                continue
            except AssertionError, ex:
                msg =  "Found Assertion error while retrieving output from worker process.\n"
                msg += str(ex)
                msg += "This indicates something critical happened to a worker process"
                msg += "We will recover what jobs we know were submitted, and resubmit the rest"
                msg += "Refreshing worker pool at end of loop"
                logging.error(msg)
                continue

            try:
                output   = res['stdout']
                error    = res['stderr']
                idList   = res['idList']
                exitCode = res['exitCode']
            except KeyError, ex:
                msg =  "Error in finding key from result pipe\n"
                msg += "Something has gone crticially wrong in the worker\n"
                try:
                    msg += "Result: %s\n" % str(res)
                except:
                    pass
                msg += str(ex)
                logging.error(msg)
                continue

            if not exitCode == 0:
                logging.error("Condor returned non-zero.  Printing out command stderr")
                logging.error(error)
                logging.error("Stdout was")
                logging.error(output)
                logging.error("Command line was:")
                logging.error(res['command'])
                errorCheck, errorMsg = parseError(error = error)
                logging.error("Processing failed jobs and proceeding to the next jobs.")
                logging.error("Do not restart component.")
            else:
                errorCheck = None

            if errorCheck:
                self.errorCount += 1
                condorErrorReport = Report()
                condorErrorReport.addError("JobSubmit", 61202, "CondorError", errorMsg)
                for jobID in idList:
                    for job in jobs:
                        if job.get('id', None) == jobID:
                            job['fwjr'] = condorErrorReport
                            failedJobs.append(job)
                            break
            else:
                if self.errorCount > 0:
                    self.errorCount -= 1
                for jobID in idList:
                    for job in jobs:
                        if job.get('id', None) == jobID:
                            successfulJobs.append(job)
                            break
                        
            self.errorCount = self.checkErrorCount( self.errorCount )
            
        return successfulJobs, failedJobs

    def checkErrorCount(self, errorCount):
        # If we get a lot of errors in a row it's probably time to
        # report this to the operators.
        if self.errorCount > self.errorThreshold:
            try:
                msg = "Exceeded errorThreshold while submitting to condor. Check condor status."
                logging.error(msg)
                logging.error("Reporting to Alert system and continuing to process jobs")
                from WMCore.Alerts import API as alertAPI
                preAlert, sender = alertAPI.setUpAlertsMessaging(self,
                                                                 compName = "BossAirRemoteCondorPlugin")
                sendAlert = alertAPI.getSendAlert(sender = sender,
                                                  preAlert = preAlert)
                sendAlert(6, msg = msg)
                sender.unregister()
                errorCount = 0
            except:
                # There's nothing we can really do here
                pass
        return errorCount

    def retrieveRemoteFiles(self, jobList):
        skipCount = 0
        for job in jobList:
            if not job.get('cache_dir', None):
                logging.error("Warning, job (id: %s) has no cache_dir!" % job['jobid'])
                skipCount += 1
                continue
            filePair = self.getTransferPair( job, self.remoteSubdir )
            scpCommand = self.makeScpInwardCommand([filePair])
            self.enqueueCommand(scpCommand, [job['jobid']])
             
        timeout = getattr(self.config.JobSubmitter, 'getTimeout', 400)
        # TODO: refactor checkCondorResponse to handle failing to scp files
        self.checkCondorResponse([], len(jobList) - skipCount, timeout)
    
    def getTransferPair(self, job, baseOutput):
        return ( '"%s/*"' % self.getRemoteOutputDir(job, baseOutput),
                 job['cache_dir'] )
        
    def track(self, jobs, info = None):
        """
        _track_

        Track the jobs while in condor
        This returns a three-way ntuple
        First, the total number of jobs still running
        Second, the jobs that need to be changed
        Third, the jobs that need to be completed
        """


        # Create an object to store final info
        retrieveList = []
        changeList   = []
        completeList = []
        runningList  = []
        noInfoFlag   = False

        # Get the jobs from condor
        jobInfo = self.getClassAds()
        # make it into a rad dict
        jobInfo = self.sortClassAdsByJobID(jobInfo)
        if not jobInfo:
            return runningList, changeList, completeList
        if len(jobInfo.keys()) == 0:
            noInfoFlag = True
        logging.info("Attempting to track %s jobs" % len(jobs))
        for job in jobs:
            # Now go over the jobs from WMBS and see what we have
            if not job['jobid'] in jobInfo.keys():
                # Two options here, either put in removed, or not
                # Only cycle through Removed if condor_q is sending
                # us no information
                retrieveList.append(job)
                if noInfoFlag:
                    if not job['status'] == 'Removed':
                        # If the job is not in removed, move it to removed
                        job['status']      = 'Removed'
                        job['status_time'] = int(time.time())
                        changeList.append(job)
                    elif time.time() - float(job['status_time']) > self.removeTime:
                        # If the job is in removed, and it's been missing for more
                        # then self.removeTime, remove it.
                        completeList.append(job)
                else:
                    completeList.append(job)
            else:
                jobAd     = jobInfo.get(job['jobid'])
                jobStatus = int(jobAd.get('JobStatus', 0))
                statName  = 'Unknown'
                if jobStatus == 1:
                    # Job is Idle, waiting for something to happen
                    statName = 'Idle'
                elif jobStatus == 5:
                    # Job is Held; experienced an error
                    statName = 'Held'
                    retrieveList.append(job)
                elif jobStatus == 2 or jobStatus == 6:
                    # Job is Running, doing what it was supposed to
                    # NOTE: Status 6 is transferring output
                    # I'm going to list this as running for now because it fits.
                    statName = 'Running'
                elif jobStatus == 3:
                    # Job is in X-state: List as error
                    statName = 'Error'
                    retrieveList.append(job)
                elif jobStatus == 4:
                    # Job is completed
                    statName = 'Complete'
                    retrieveList.append(job)
                else:
                    # What state are we in?
                    retrieveList.append(job)
                    logging.info("Job in unknown state %i" % jobStatus)

                # Get the global state
                job['globalState'] = RemoteCondorPlugin.stateMap()[statName]

                if statName != job['status']:
                    # Then the status has changed
                    job['status']      = statName
                    job['status_time'] = 0

                #Check if we have a valid status time
                if not job['status_time']:
                    if job['status'] == 'Running':
                        job['status_time'] = jobAd.get('JobStartDate', 0)
                    elif job['status'] == 'Idle':
                        job['status_time'] = jobAd.get('QDate', 0)
                    else:
                        job['status_time'] = jobAd.get('EnteredCurrentStatus', 0)
                    changeList.append(job)

                runningList.append(job)

        return runningList, changeList, completeList

    def complete(self, jobs):
        """
        Do any completion work required

        In this case, look for a returned logfile
        """
        logging.error("completion!!!")
        
        # retrieve files
        timeout = getattr(self.config.JobSubmitter, 'getTimeout', 400)
        self.checkAndStartPool( timeout )        
        self.retrieveRemoteFiles( jobs )
        self.close()
        
        
        for job in jobs:
            if job.get('cache_dir', None) == None or job.get('retry_count', None) == None:
                # Then we can't do anything
                logging.error("Can't find this job's cache_dir in RemoteCondorPlugin.complete")
                logging.error("cache_dir: %s" % job.get('cache_dir', 'Missing'))
                logging.error("retry_count: %s" % job.get('retry_count', 'Missing'))
                continue
            reportName = os.path.join(job['cache_dir'], 'Report.%i.pkl' % job['retry_count'])
            if os.path.isfile(reportName) and os.path.getsize(reportName) > 0:
                # Then we have a real report.
                # Do nothing
                continue
            if os.path.isdir(reportName):
                # Then something weird has happened.
                # File error, do nothing
                logging.error("Went to check on error report for job %i.  Found a directory instead.\n" % job['id'])
                logging.error("Ignoring this, but this is very strange.\n")

            # If we're still here, we must not have a real error report
            logOutput = 'Could not find jobReport\n'
            #But we don't know exactly the condor id, so it will append
            #the last lines of the latest condor log in cache_dir
            genLogPath = os.path.join(job['cache_dir'], 'condor.*.*.log')
            logPaths = glob.glob(genLogPath)
            errLog = None
            if len(logPaths):
                errLog = max(logPaths, key = lambda path :
                                                    os.stat(path).st_mtime)
            if errLog != None and os.path.isfile(errLog):
                logTail = BasicAlgos.tail(errLog, 50)
                logOutput += 'Adding end of condor.log to error message:\n'
                logOutput += '\n'.join(logTail)
            if not os.path.isdir(job['cache_dir']):
                msg =  "Serious Error in Completing condor job with id %s!\n" % job.get('id', 'unknown')
                msg += "Could not find jobCache directory - directory deleted under job: %s\n" % job['cache_dir']
                msg += "Creating artificial cache_dir for failed job report\n"
                logging.error(msg)
                if not os.path.exists(job['cache_dir']):
                    os.makedirs(job['cache_dir'])
                logOutput += msg
                condorReport = Report()
                condorReport.addError("NoJobReport", 99304, "NoCacheDir", logOutput)
                reportDir = os.path.dirname(reportName)
                if not os.path.exists(reportDir):
                    os.makedirs(reportDir)
                try:
                    condorReport.save(filename = reportName)
                except IOError:
                    # not sure how this happens..
                    logging.critical("Tried to save a file where we had made a directory, and it bombed")
                    pass
                continue
            condorReport = Report()
            condorReport.addError("NoJobReport", 99303, "NoJobReport", logOutput)
            if os.path.isfile(reportName):
                # Then we have a file already there.  It should be zero size due
                # to the if statements above, but we should remove it.
                if os.path.getsize(reportName) > 0:
                    # This should never happen.  If it does, ignore it
                    msg =  "Critical strange problem.  FWJR changed size while being processed."
                    logging.error(msg)
                else:
                    try:
                        os.remove(reportName)
                        condorReport.save(filename = reportName)
                    except Exception, ex:
                        logging.error("Cannot remove and replace empty report %s" % reportName)
                        logging.error("Report continuing without error!")
            else:
                condorReport.save(filename = reportName)

            # Debug message to end loop
            logging.debug("No returning job report for job %i" % job['id'])


        return

    def applyCommand(self, condorCommand, jobs, info = None, constraint = None):
        """
        applies a given condor_X command to either a list of jobs or a given
        constraint
        FIXME - mid-refactor, only accepts a constraint
        """
        command = [self.ssh, self.remoteUserHost]
        command.extend( self.gsisshOptions )
        # TODO: batch these commands
        command.append(" ".join([condorCommand, '-constraint',\
                                "'%s'" % constraint]))
        proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                stdout = subprocess.PIPE, shell = False)
        stdout, stderr = proc.communicate()
        if not proc.returncode == 0:
            # Then things have gotten bad - condor_rm is not responding
            logging.error("%s returned non-zero value %s" % (command, str(proc.returncode)))
            logging.error("command")
            logging.error(command)
            logging.error("stdout")
            logging.error(stdout)
            logging.error("stderr")
            logging.error(stderr)
            logging.error("Skipping classAd processing this round")
            return False

        return True



    def kill(self, jobs, info = None, constraint = None):
        """
        Kill a list of jobs based on the WMBS job names

        """
        if not constraint:
            raise RuntimeError, "RemoteCondorPlugin needs a constraint with which to kill"

        for job in jobs:
            jobID = job['jobid']
            command = [self.ssh, self.remoteUserHost]
            command.extend( self.gsisshOptions )
            # TODO: batch these commands
            command.append(" ".join([command, '-constraint',\
                                     constraint]))
            proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                    stdout = subprocess.PIPE, shell = False)
            stdout, stderr = proc.communicate()
            if not proc.returncode == 0:
                # Then things have gotten bad - condor_rm is not responding
                logging.error("%s returned non-zero value %s" % (command, str(proc.returncode)))
                logging.error("command")
                logging.error(command)
                logging.error("stdout")
                logging.error(stdout)
                logging.error("stderr")
                logging.error(stderr)
                logging.error("Skipping classAd processing this round")
                return False

        return True

    def hold(self, constraint = None):
        """
        Hold a list of jobs -- only accepts a constraint
        """
        return self.applyCommand('condor_hold', [], constraint = constraint)

    # Start with submit functions

    def initSubmit(self, jobList=None, remoteScript = None):
        """
        _makeConfig_

        Make common JDL header
        """
        jdl = []


        # -- scriptFile & Output/Error/Log filenames shortened to
        #    avoid condorg submission errors from > 256 character pathnames

        jdl.append("universe = vanilla\n")
        jdl.append("requirements = %s\n" % self.reqStr)

        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        if not remoteScript:
            jdl.append("Executable = %s\n" % self.scriptFile)
        else:
            jdl.append("Executable = %s\n" % remoteScript)
        jdl.append("Output = condor.$(Cluster).$(Process).out\n")
        jdl.append("Error = condor.$(Cluster).$(Process).err\n")
        jdl.append("Log = condor.$(Cluster).$(Process).log\n")
        jdl.append("PeriodicRemove = ( ( JobStatus == 2 ) && ( ( ( CurrentTime - JobCurrentStartDate ) > ( MaxWallTimeMins * 60 ) ) =?= true ) ) || ( JobStatus == 5 && ( CurrentTime - EnteredCurrentStatus ) > 691200 ) || ( JobStatus == 1 && ( CurrentTime - EnteredCurrentStatus ) > 691200 )\n")
        jdl.append("+WMAgent_AgentName = \"%s\"\n" %(self.agent))
        # TODO: some stuff can overflow just fine
        jdl.append("+CMS_ALLOW_OVERFLOW = False\n")
        jdl.append("+JOB_Is_ITB = False\n")
        jdl.extend(self.customizeCommon(jobList))

        if self.proxy:
            # Then we have to retrieve a proxy for this user
            job0   = jobList[0]
            userDN = job0.get('userdn', None)
            if not userDN:
                # Then we can't build ourselves a proxy
                logging.error("Asked to build myProxy plugin, but no userDN available!")
                logging.error("Checked job %i" % job0['id'])
                return jdl
            logging.info("Fetching proxy for %s" % userDN)
            # Build the proxy
            # First set the userDN of the Proxy object
            self.proxy.userDN = userDN
            # Second, get the actual proxy
            if self.serverHash:
                # If we built our own serverHash, we have to be able to send it in
                filename = self.proxy.logonRenewMyProxy(credServerName = self.serverHash)
            else:
                # Else, build the serverHash from the proxy sha1
                filename = self.proxy.logonRenewMyProxy()
            logging.info("Proxy stored in %s" % filename)
            if self.glexecPath:
                self.jdlProxyFile = '%s.user' % filename
                self.glexecProxyFile = filename
                command = 'export GLEXEC_CLIENT_CERT=%s; export GLEXEC_SOURCE_PROXY=%s; export X509_USER_PROXY=%s; ' % \
                          (self.glexecProxyFile, self.glexecProxyFile, self.glexecProxyFile) + \
                          'export GLEXEC_TARGET_PROXY=%s; %s /usr/bin/id' % \
                          (self.jdlProxyFile, self.glexecPath)
                proc = subprocess.Popen(command, stderr = subprocess.PIPE,
                                        stdout = subprocess.PIPE, shell = True)
                out, err = proc.communicate()
                logging.info("Created new user proxy with glexec %s" % self.jdlProxyFile)
            else:
                self.jdlProxyFile = filename
            jdl.append("x509userproxy = %s\n" % self.jdlProxyFile)

        return jdl

    def customizeCommon(self, jobList):
        """
        JDL additions just for this implementation. Over-ridden in sub-classes
        These are the Glide-in specific bits
        """
        jdl = []
        jdl.append('+DESIRED_Archs = \"INTEL,X86_64\"\n')
        jdl.append('+REQUIRES_LOCAL_DATA = True\n')

        # Check for multicore
        if jobList and jobList[0].get('taskType', None) in self.multiTasks:
            jdl.append('+DESIRES_HTPC = True\n')
        else:
            jdl.append('+DESIRES_HTPC = False\n')
        
        if jobList and jobList[0].get('taskType', None) in self.overflowTasks:
            jdl.append('+CMS_ALLOW_OVERFLOW = True\n')
        else:
            jdl.append('+CMS_ALLOW_OVERFLOW = False\n')

        return jdl

    def getRemoteOutputDir(self, job, baseOutput):
        target = baseOutput
        if baseOutput and not baseOutput.endswith('/'):
            target += '/'
        
        if not job['cache_dir']:
            print "no cache dir?"
            
        target += job['cache_dir']

        if target.startswith('/'):
            target = target[1:]
        return target
    
    def makeJDL(self, jobList, remoteSubmitScript, baseOutput = ""):
        """
        _makeJDL_

        For a given job/cache/spec make a JDL fragment to submit the job

        """
        requiredDirs = []
        
        if len(jobList) < 1:
            #I don't know how we got here, but we did
            logging.error("No jobs passed to plugin")
            return None

        jdl = self.initSubmit(jobList, remoteSubmitScript)


        # For each script we have to do queue a separate directory, etc.
        for job in jobList:
            if job == {}:
                # Then I don't know how we got here either
                logging.error("Was passed a nonexistant job.  Ignoring")
                continue
            # jdl.append("initialdir = %s\n" % job['cache_dir'])
            jdl.append("transfer_input_files = %s\n" % ", ".join(job['remoteFiles']))
            argString = "arguments = %s %i\n" \
                        % (os.path.basename(job['sandbox']), job['id'])
            jdl.append(argString)

            jdl.extend(self.customizePerJob(job))

            # Transfer the output files
            jobOutput = self.getRemoteOutputDir( job, baseOutput )
            requiredDirs.append( jobOutput )    
            jdl.append("transfer_output_files = Report.%i.pkl\n" % (job["retry_count"]))
            jdl.append("transfer_output_remaps = \"Report.%i.pkl = %s/Report.%i.pkl\"\n" %
                            (job["retry_count"], jobOutput,job["retry_count"]))
            jdl.append("Output = %s/condor.$(Cluster).$(Process).out\n" % jobOutput)
            jdl.append("Error = %s/condor.$(Cluster).$(Process).err\n" % jobOutput)
            jdl.append("Log = %s/condor.$(Cluster).$(Process).log\n" % jobOutput)
            
            # Add priority if necessary
            if job.get('priority', None) == None:
                job['priority'] = 0
            if job.get('priority', None) != None:
                try:
                    prio = int(job['priority']) + 6
                    jdl.append("priority = %i\n" % prio)
                except ValueError:
                    logging.error("Priority for job %i not castable to an int\n" % job['id'])
                    logging.error("Not setting priority")
                    logging.debug("Priority: %s" % job['priority'])
                except Exception, ex:
                    logging.error("Got unhandled exception while setting priority for job %i\n" % job['id'])
                    logging.error(str(ex))
                    logging.error("Not setting priority")

            jdl.append("+WMAgent_JobID = %s\n" % job['jobid'])

            jdl.append("Queue 1\n")

        return jdl, requiredDirs

    def customizePerJob(self, job):
        """
        JDL additions just for this implementation. Over-ridden in sub-classes
        These are the Glide-in specific bits
        """
        jdl = []
        jobCE = job['location']
        if not jobCE:
            # Then we ended up with a site that doesn't exist?
            logging.error("Job for non-existant site %s" \
                            % (job['location']))
            return jdl

        if self.useGSite:
            jdl.append('+GLIDEIN_CMSSite = \"%s\"\n' % (jobCE))
        if self.submitWMSMode and len(job.get('possibleSites', [])) > 0:
            strg = list(job.get('possibleSites')).__str__().lstrip('[').rstrip(']')
            seList = []
            strg = filter(lambda c: c not in "\'", strg)
            siteSplit = strg.split(', ')
            for site in siteSplit:
                seList.extend(self.siteDB.cmsNametoSE(site))
            jdl.append('+DESIRED_Sites = \"%s\"\n' % strg)
            jdl.append('+DESIRED_SEs = \"%s\"\n' % ','.join(seList))
        else:
            jdl.append('+DESIRED_Sites = \"%s\"\n' %(jobCE))

        if job.get('proxyPath', None):
            jdl.append('x509userproxy = %s\n' % job['proxyPath'])

        if job.get('requestName', None):
            jdl.append('+WMAgent_RequestName = "%s"\n' % job['requestName'])

        return jdl

    def getCEName(self, jobSite):
        """
        _getCEName_

        This is how you get the name of a CE for a job
        """

        if not jobSite in self.locationDict.keys():
            siteInfo = self.locationAction.execute(siteName = jobSite)
            self.locationDict[jobSite] = siteInfo[0].get('ce_name', None)
        return self.locationDict[jobSite]

    def getClassAds(self, constraint = None, attribs = None):
        """
        _getClassAds_

        Grab classAds from condor_q using xml parsing
        """
        if not constraint:
            constraint = '\'WMAgent_AgentName == "%s" && WMAgent_JobID =!= UNDEFINED\''
        if not attribs:
            attribs = [ 'JobStatus', 'EnteredCurrentStatus',\
                        'JobStartDate', 'QDate', 'WMAgent_JobID' ]
        jobInfo = {}
        command = [self.ssh, self.remoteUserHost]
        command.extend( self.gsisshOptions )
        extraString = "condor_q -constraint '%s' " % constraint
        attribList = []
        for attrib in attribs[:-1]:
            attribList.extend(['-format', '"(%s:%%s)  "' % attrib, attrib])
        # The last one has to be handled special to get the end of record thing
        attribList.extend(['-format', '"(%s:%%s) :::"'%attribs[-1], attribs[-1]])
        extraString += " ".join(attribList)
        command.append(extraString)
        print "executing command %s" % " ".join(command)
        pipe = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = False)
        stdout, stderr = pipe.communicate()
        classAdsRaw = stdout.split(':::')
        if not pipe.returncode == 0:
            # Then things have gotten bad - condor_q is not responding
            logging.error("condor_q returned non-zero value %s" % str(pipe.returncode))
            logging.error("command")
            logging.error(command)
            logging.error("stdout")
            logging.error(stdout)
            logging.error("stderr")
            logging.error(stderr)
            logging.error("Skipping classAd processing this round")
            return None


        if classAdsRaw == '':
            # We have no jobs
            return jobInfo
        
        classAdList = []
        for ad in classAdsRaw:
            # There should be one for every job
            if not re.search("\(", ad):
                # There is no ad.
                # Don't know what happened here
                continue
            statements = ad.split('(')
            tmpDict = {}
            for statement in statements:
                # One for each value
                if not re.search(':', statement):
                    # Then we have an empty statement
                    continue
                key = str(statement.split(':')[0])
                value = statement.split(':')[1].split(')')[0]
                tmpDict[key] = value
            classAdList.append(tmpDict)
        return classAdList

    def sortClassAdsByJobID(inputList):
        jobInfo = {}
        for tmpDict in inputList:
            if not 'WMAgent_JobID' in tmpDict.keys():
                # Then we have an invalid job somehow
                logging.error("Invalid job discovered in condor_q")
                logging.error(tmpDict)
                continue
            else:
                jobInfo[int(tmpDict['WMAgent_JobID'])] = tmpDict

        logging.info("Retrieved %i classAds" % len(jobInfo))
        return jobInfo

