#!/usr/bin/python
# -*- coding: utf-8 -*-

#
# Copyright (C) 1994-2019 Altair Engineering, Inc.
# For more information, contact Altair at www.altair.com.
#
# This file is part of the PBS Professional ("PBS Pro") software.
#
# Open Source License Information:
#
# PBS Pro is free software. You can redistribute it and/or modify it under the
# terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# PBS Pro is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.
# See the GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Commercial License Information:
#
# For a copy of the commercial license terms and conditions,
# go to: (http://www.pbspro.com/UserArea/agreement.html)
# or contact the Altair Legal Department.
#
# Altair’s dual-license business model allows companies, individuals, and
# organizations to create proprietary derivative works of PBS Pro and
# distribute them - whether embedded or bundled with other software -
# under a commercial license agreement.
#
# Use of Altair’s trademarks, including but not limited to "PBS™",
# "PBS Professional®", and "PBS Pro™" and Altair’s logos is subject to Altair's
# trademark licensing policies.
#

'''
qmgr -c "c h resource_framework_hook"
qmgr -c "s h resource_framework_hook enabled=t"
qmgr -c "s h resource_framework_hook event = 'execjob_begin,execjob_epilogue,execjob_end,exechost_periodic'" 
qmgr -c "i h resource_framework_hook application/x-python default resource_framework_hook.py"
qmgr -c "i h resource_framework_hook application/x-config default resource_framework_hook.json"
'''

'''
qmgr -c "c r GPU_memoryClock_average type=string"
qmgr -c "c r GPU_smClock_minValue type=string"
qmgr -c "c r GPU_memoryUtilization_maxValue type=string"
qmgr -c "c r GPU_smClock_maxValue type=string"
qmgr -c "c r GPU_smUtilization_average type=string"
qmgr -c "c r GPU_maxGpuMemoryUsed type=string"
qmgr -c "c r GPU_smUtilization_maxValue type=string"
qmgr -c "c r GPU_smClock_average type=string"
qmgr -c "c r GPU_startTime type=string"
qmgr -c "c r GPU_smUtilization_minValue type=string"
qmgr -c "c r GPU_endTime type=string"
qmgr -c "c r GPU_energyConsumed type=string"
qmgr -c "c r GPU_memoryClock_minValue type=string"
qmgr -c "c r GPU_memoryClock_maxValue type=string"
qmgr -c "c r GPU_memoryUtilization_minValue type=string"
qmgr -c "c r GPU_memoryUtilization_average type=string"
qmgr -c "c r GPU_duration type=string"

qmgr -c "c r GPU_memoryClock_average_per_node type=string"
qmgr -c "c r GPU_smClock_minValue_per_node type=string"
qmgr -c "c r GPU_memoryUtilization_maxValue_per_node type=string"
qmgr -c "c r GPU_smClock_maxValue_per_node type=string"
qmgr -c "c r GPU_smUtilization_average_per_node type=string"
qmgr -c "c r GPU_maxGpuMemoryUsed_per_node type=string"
qmgr -c "c r GPU_smUtilization_maxValue_per_node type=string"
qmgr -c "c r GPU_smClock_average_per_node type=string"
qmgr -c "c r GPU_startTime_per_node type=string"
qmgr -c "c r GPU_smUtilization_minValue_per_node type=string"
qmgr -c "c r GPU_endTime_per_node type=string"
qmgr -c "c r GPU_energyConsumed_per_node type=string"
qmgr -c "c r GPU_memoryClock_minValue_per_node type=string"
qmgr -c "c r GPU_memoryClock_maxValue_per_node type=string"
qmgr -c "c r GPU_memoryUtilization_minValue_per_node type=string"
qmgr -c "c r GPU_memoryUtilization_average_per_node type=string"
qmgr -c "c r GPU_duration_per_node type=string"

qmgr -c "c r GPU_memoryClock_average_per_node_gpu type=string"
qmgr -c "c r GPU_smClock_minValue_per_node_gpu type=string"
qmgr -c "c r GPU_memoryUtilization_maxValue_per_node_gpu type=string"
qmgr -c "c r GPU_smClock_maxValue_per_node_gpu type=string"
qmgr -c "c r GPU_smUtilization_average_per_node_gpu type=string"
qmgr -c "c r GPU_maxGpuMemoryUsed_per_node_gpu type=string"
qmgr -c "c r GPU_smUtilization_maxValue_per_node_gpu type=string"
qmgr -c "c r GPU_smClock_average_per_node_gpu type=string"
qmgr -c "c r GPU_startTime_per_node_gpu type=string"
qmgr -c "c r GPU_smUtilization_minValue_per_node_gpu type=string"
qmgr -c "c r GPU_endTime_per_node_gpu type=string"
qmgr -c "c r GPU_energyConsumed_per_node_gpu type=string"
qmgr -c "c r GPU_memoryClock_minValue_per_node_gpu type=string"
qmgr -c "c r GPU_memoryClock_maxValue_per_node_gpu type=string"
qmgr -c "c r GPU_memoryUtilization_minValue_per_node_gpu type=string"
qmgr -c "c r GPU_memoryUtilization_average_per_node_gpu type=string"
qmgr -c "c r GPU_duration_per_node_gpu type=string"

'''

import pbs
import fcntl
import sys
import os
import shutil
import time
import re
import math
import datetime
import copy
    
    

# Check if GPU is configured on the machine
if not os.path.isfile('/usr/bin/nvidia-smi'):
    pbs.logmsg(pbs.LOG_WARNING, "Running hook %s on non GPU node. You can either disable hook from node or exclude it." % (pbs.event().hook_name))
    sys.exit(0)


# Define PBS_CONF_FILE variable

if sys.platform == 'win32':
    os.environ['PBS_CONF_FILE'] = 'C:\Program Files\PBS Pro\pbs.conf'
else:
    os.environ['PBS_CONF_FILE'] = '/etc/pbs.conf'

if os.path.isfile(os.environ['PBS_CONF_FILE']):
    pbs_conf = open(os.environ['PBS_CONF_FILE'], 'r')
    for line in pbs_conf:
        if '=' in line:
            os.environ[line.split('=')[0]] = line.split('=')[1].strip('\n')
    pbs_conf.close()
else:
    print 'Unable to find PBS_CONF_FILE ... ' + os.environ['PBS_CONF_FILE']
    sys.exit(1)

pbsExec = os.environ['PBS_EXEC']
pbsHome = os.environ['PBS_HOME']


try:
    pbs_version_path = pbsHome+'/pbs_version'

    pbs_version = open(pbs_version_path, 'r')
    supported_pbs = pbs_version.read().split('.')[0]
    
    if supported_pbs in ['13','14','18','19']:
        using_supported_pbs = True
    else:
        using_supported_pbs = False
except:
    using_supported_pbs = False


pbs.logmsg(pbs.EVENT_DEBUG, "PBS_VERSION:%s" % (supported_pbs))
#pbs.logmsg(pbs.EVENT_DEBUG, "using_supported_pbs:%s" % (str(using_supported_pbs)))

if using_supported_pbs:
    try:
        import simplejson as json
    except:
        import json
else:
    import json

    
hook_storage_path = pbsHome+'/mom_priv/hooks/hook_data/'
job_resource_results_file = 'job_resource_results'
nvidia_dcgm_lock_file = pbsHome+'/mom_priv/'+'dcgm.lock'
imported_dcgm_modules = True

try:
    if not os.path.exists(hook_storage_path):
        os.makedirs(hook_storage_path)
except OSError:
    pbs.event().reject('%s hook failed while creating hook storage path. Please contact Admin. ' % (pbs.event().hook_name))

try:
    path =  os.path.join(pbsExec, 'python', 'lib', 'python2.5', 'lib-dynload')
    if path not in sys.path:
        sys.path.append(path)
        
    path =  os.path.join(pbsExec, 'python', 'lib', 'python2.5')
    if path not in sys.path:
        sys.path.append(path)

    import subprocess
except:
    pbs.event().reject('%s hook failed with %s while importing modules. Please contact Admin. ' % (pbs.event().hook_name, sys.exc_info()[:2]))


# Resource classes
# ----------------

if not using_supported_pbs:
    try:
        path = os.path.join(os.sep, 'usr', 'src', 'dcgm', 'bindings')
        if path not in sys.path:
            sys.path.append(path)
    
        import pydcgm
        import dcgm_structs
        import dcgm_fields
        import dcgm_agent
        import dcgmvalue
    
    except:
        imported_dcgm_modules = False


class resource(object):
    def __init__(self, resource_type):
        config = parse_config_file()
        self.resource = resource_type

        self.resource_config = config['resources'][self.resource]
        
        if 'missing_value_string' in config['resources'][self.resource].keys():
            self.missing_value_string = config['resources'][self.resource]['missing_value_string']
        else:
            self.missing_value_string = ''
            
        self.resource_string = ''
        self.aggregate_resource_string = ''


    def health_check(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

        
    def diagnostic_check(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))


    def start_resource_monitoring(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))
    
    
    def get_resources_used(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

    
    def cleanup_check(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

        
    def aggregate_resources(self, res_dir, results_files):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

        # node1:(res=val)+node2(...)
        final_res = []

        for res_file in sorted(results_files):
            f = open(os.path.join(res_dir, res_file))
            res_str = f.readline()
            f.close()
            final_res.append(res_str.strip())
        
        self.aggregate_resource_string = '%s+%s' % (self.aggregate_resource_string, '+'.join(final_res))
        pbs.logmsg(pbs.EVENT_DEBUG, "self.aggregate_resource_string:%s" % (self.aggregate_resource_string))

                
    def set_mother_superior_node_resources(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))
        self.aggregate_resource_string = self.resource_string
        pbs.logmsg(pbs.EVENT_DEBUG, "self.aggregate_resource_string:%s" % (self.aggregate_resource_string))
        
                
    def add_missing_node_resources(self, missing_sister_nodes, node_resources):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))
       
       
    def set_resources_used(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

        e = pbs.event()
        j = e.job
    
        if self.aggregate_resource_string:
            j.resources_used[self.resource_config['resources'][self.resource]['PBS_resource']+'_per_node'] = self.aggregate_resource_string
            pbs.logmsg(pbs.EVENT_DEBUG, "self.aggregate_resource_string:%s" % (self.aggregate_resource_string))

    
class ngpus(resource):
    def __init__(self):
        resource.__init__(self, 'ngpus')

        config = parse_config_file()
        self.using_cgroups = config['resources'][self.resource]['using_cgroups']
        self.path_to_pbs_scripts = config['resources'][self.resource]['path_to_pbs_scripts']
        self.devices_on_host,gpu_dict = self.get_nvidia_devices_on_host()
        self.exclude_host = exclude_host(config)
        self.imported_dcgm_modules = imported_dcgm_modules
        self.gpu_ids_by_type = []
        for type in gpu_dict:
            #pbs.logmsg(pbs.LOG_DEBUG, "gpu_dict: %s" % (type))
            self.gpu_ids_by_type.append(gpu_dict[type])

        self.validation_enabled = config['enable_validation']
        self.validation_mode    = config['validation_mode']
        if self.validation_mode not in ['Error','Warning'] or self.validation_mode == None:
            self.validation_mode = 'Warning'
    
    def health_check(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

        e = pbs.event()
        try:
            job_id = e.job.id
        except:
            job_id = None

        # Check if node has GPUs
        if not self.devices_on_host:
            e.accept()

        # Check if this host will participate 
        if self.exclude_host:
            pbs.logmsg(pbs.EVENT_DEBUG, "Host is excluded.")
            e.accept()

        # Check if NVIDIA modules imported ok
        if not self.imported_dcgm_modules:
            comment = 'Error: Importing Nvidia DCGM modules failed'
            pbs.logmsg(pbs.LOG_DEBUG, comment)
            offline_vnodes(comment)
            e.reject(comment)
        
        health = ''
        comment = ''
        
        if self.validation_enabled:
            health = self.validation_mode
        else:
            if not using_supported_pbs:
                dcgmHandle = pydcgm.DcgmHandle(ipAddress='127.0.0.1', opMode=dcgm_structs.DCGM_OPERATION_MODE_AUTO)
           
                ## Get a handle to the system level object for DCGM
                dcgmSystem = dcgmHandle.GetSystem()
            
                ## Create a default group. (Default group is comprised of all the GPUs on the node)
                ## Let's call the group as "all_gpus_group". The method returns an opaque handle (groupId) to
                ## identify the newly created group.
                dcgmGroup = pydcgm.DcgmGroup(dcgmHandle, groupName="all_gpus_group", groupType=dcgm_structs.DCGM_GROUP_DEFAULT)
            
                ## Invoke method to get gpu IDs of the members of the newly-created group
                groupGpuIds = dcgmGroup.GetGpuIds()
                pbs.logmsg(pbs.EVENT_DEBUG, "groupGpuIds %s" % (groupGpuIds))
        
                ## Add the health watches
                newSystems = dcgm_structs.DCGM_HEALTH_WATCH_ALL
                dcgmGroup.health.Set(newSystems)
                dcgmSystem.UpdateAllFields(1)
        
                try:
                    ## Invoke Health checks
                    group_health = dcgmGroup.health.Check()
                    #pbs.logmsg(pbs.EVENT_DEBUG, "group_health %s" % (group_health))
                    
                    if group_health.overallHealth == dcgm_structs.DCGM_HEALTH_RESULT_PASS:
                        health = 'Pass'
                        
                    elif group_health.overallHealth == dcgm_structs.DCGM_HEALTH_RESULT_WARN:
                        health = 'Warning'
                        
                    elif group_health.overallHealth == dcgm_structs.DCGM_HEALTH_RESULT_FAIL:
                        health = 'Error'
                        
                        self.delete_group(dcgmHandle, dcgmGroup)
                    else :
                        health = 'N/A'
                        comment = group_health
        
                except dcgm_structs.DCGMError:
                    errorCode = dcgm_structs.DCGMError.value
                    pbs.logmsg(pbs.EVENT_DEBUG, "dcgmHealthCheck returned error: %d" % (errorCode))
                    sys.exc_clear()
                    health = 'Error'
        
                self.delete_group(dcgmHandle, dcgmGroup)
            else:
                cmd = [self.path_to_pbs_scripts+'/nvidia_health_check']
            
                pbs.logmsg(pbs.EVENT_DEBUG, "cmd=%s" % (cmd))
                process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output, err = process.communicate()
                pbs.logmsg(pbs.EVENT_DEBUG, "output=%s" % (output.strip()))
                pbs.logmsg(pbs.EVENT_DEBUG, "err=%s" % (err))
    
                output = json.loads(output)
                health = output['health']
                comment = output['comment']

        
        if health == 'Pass':
            pbs.logmsg(pbs.EVENT_DEBUG, "Pass")
            
        elif health == 'Warning':
            if job_id:
                message = 'Warning: Nvidia health check returned warnings, allowing new job %s to run' % (job_id)
            else:
                message = 'Warning: Nvidia health check returned warnings'
            
            if comment:
                message += '. %s' % (comment)

            pbs.logmsg(pbs.LOG_DEBUG, message)
            offline_vnodes(message)
            
        elif health == 'Error':
            if job_id:
                message = 'Error: Nvidia health check failed while running job %s' % (job_id)
            else:
                message = 'Error: Nvidia health check failed'

            if comment:
                message += '. %s' % (comment)

            pbs.logmsg(pbs.LOG_DEBUG, message)
            offline_vnodes(message)
            e.reject(message)
            
        else :
            message = "N/A"
            if comment:
                message += '. %s' % (comment)

            pbs.logmsg(pbs.EVENT_DEBUG, message)
    
    
    def diagnostic_check(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

        e = pbs.event()
        try:
            job_id = e.job.id
        except:
            job_id = None

        # Check if node has GPUs
        if not self.devices_on_host:
            e.accept()

        # Check if this host will participate 
        if self.exclude_host:
            pbs.logmsg(pbs.EVENT_DEBUG, "Host is excluded.")
            e.accept()

        # Check if NVIDIA modules imported ok
        if not self.imported_dcgm_modules:
            comment = 'Error: Importing Nvidia DCGM modules failed'
            pbs.logmsg(pbs.LOG_DEBUG, comment)
            offline_vnodes(comment)
            e.reject(comment)
        
        gpu_ids = self.gpu_ids_by_type

        health_info = []

        if not using_supported_pbs:
            dcgmHandle = pydcgm.DcgmHandle(ipAddress='127.0.0.1', opMode=dcgm_structs.DCGM_OPERATION_MODE_AUTO)
       
            ## Get a handle to the system level object for DCGM
            dcgmSystem = dcgmHandle.GetSystem()
        
            ## Create a default group. (Default group is comprised of all the GPUs on the node)
            ## Let's call the group as "all_gpus_group". The method returns an opaque handle (groupId) to
            ## identify the newly created group.
            dcgmGroup = pydcgm.DcgmGroup(dcgmHandle, groupName="all_gpus_group", groupType=dcgm_structs.DCGM_GROUP_DEFAULT)
        
            ## Invoke method to get gpu IDs of the members of the newly-created group
            groupGpuIds = dcgmGroup.GetGpuIds()
            pbs.logmsg(pbs.EVENT_DEBUG, "groupGpuIds %s" % (groupGpuIds))
    
            ## This will go ahead and perform a "prologue" diagnostic
            ## to make sure everything is ready to run
            ## currently this calls an outside diagnostic binary but eventually
            ## that binary will be merged into the DCGM framework
            ## The "response" is a dcgmDiagResponse structure that can be parsed for errors.
            response = dcgmGroup.action.RunDiagnostic(dcgm_structs.DCGM_DIAG_LVL_SHORT)
            #pbs.logmsg(pbs.EVENT_DEBUG, "response %s" % (response))
                    
            if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.blacklist):
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed blacklist")
                health_info.append('blacklist')
            if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.nvmlLibrary):
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed nvmlLibrary")
                health_info.append('nvmlLibrary')
            if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.cudaMainLibrary):
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed cudaMainLibrary")
                health_info.append('cudaMainLibrary')
            #if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.cudaRuntimeLibrary):
            #    pbs.logmsg(pbs.EVENT_DEBUG, "Failed cudaRuntimeLibrary")
            #    health_info.append('cudaRuntimeLibrary')
            if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.permissions):
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed permissions")
                health_info.append('permissions')
            if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.persistenceMode):
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed persistenceMode")
                health_info.append('persistenceMode')
            if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.environment):
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed environment")
                health_info.append('environment')
            if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.pageRetirement):
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed pageRetirement")
                health_info.append('pageRetirement')
            if (dcgm_structs.DCGM_DIAG_RESULT_PASS !=  response.graphicsProcesses):
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed graphicsProcesses")
                health_info.append('graphicsProcesses')
        
    
            self.delete_group(dcgmHandle, dcgmGroup)
        else:
            cmd = [self.path_to_pbs_scripts+'/nvidia_diagnostic_check', '-i', json.dumps(gpu_ids)]
        
            pbs.logmsg(pbs.EVENT_DEBUG, "cmd=%s" % (cmd))
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, err = process.communicate()
            pbs.logmsg(pbs.EVENT_DEBUG, "output=%s" % (output.strip()))
            pbs.logmsg(pbs.EVENT_DEBUG, "err=%s" % (err))

            health_info = json.loads(output)
        

        if health_info:
            if job_id:
                comment = 'Error: while running job %s the following Nvidia diagostic tests failed: %s' % (job_id, ', '.join(health_info))
            else:
                comment = 'Error: the following Nvidia diagostic tests failed: %s' % (', '.join(health_info))
    
            pbs.logmsg(pbs.LOG_DEBUG, comment)
            offline_vnodes(comment)
            e.reject(comment)
        else:
            pbs.logmsg(pbs.EVENT_DEBUG, "Pass")


    def start_resource_monitoring(self):
        e = pbs.event()
        j = e.job
        job_id = j.id

        def walltime_to_secs(walltime):
            wt = walltime.split(':')
        
            if len(wt) == 3:
                try:
                    wt[0] = int(wt[0])
                except:
                    wt[0] = 0
                try:
                    wt[1] = int(wt[1])
                except:
                    wt[1] = 0
                try:
                    wt[2] = int(wt[2])
                except:
                    wt[2] = 0
                wt_secs = wt[0] * 3600 + wt[1] * 60 + wt[2]
            elif len(wt) == 2:
                try:
                    wt[0] = int(wt[0])
                except:
                    wt[0] = 0
                try:
                    wt[1] = int(wt[1])
                except:
                    wt[1] = 0
                wt_secs = wt[0] * 60 + wt[1]
            elif len(wt) == 1:
                try:
                    wt[0] = int(wt[0])
                except:
                    wt[0] = 0
                wt_secs = wt[0]
            else:
                wt_secs = 0
        
            return wt_secs
    
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))
        
        # Find server default walltime if defined
        try:
            default_walltime = pbs.server().resources_default['walltime']
        except:
            default_walltime = '8760:00:00'
    
        walltime = j.Resource_List['walltime']
        if walltime == None:
            # Need to use system detault walltime if none specified with job
            walltime = str(default_walltime)
        else:
            walltime = str(walltime)
    
        walltime_secs = walltime_to_secs(walltime)
        pbs.logmsg(pbs.EVENT_DEBUG, "walltime_secs=%s" % (walltime_secs))
        
        nvidia_devices = self.get_nvidia_devices(job_id)

        if not using_supported_pbs:
        
            handleObj, groupObj = self.setup_dcgm_group(job_id, nvidia_devices)
            
            j.Variable_List['nvidia_group_id'] = str(groupObj.GetId().value)
            pbs.logmsg(pbs.EVENT_DEBUG, "nvidia_group_id=%s" % (j.Variable_List['nvidia_group_id']))
            
            groupObj.stats.WatchJobFields(1000000, walltime_secs, 0)

            # Notify DCGM to start collecting stats
            groupObj.stats.StartJobStats(job_id)
        
            #self.delete_group(handleObj, groupObj)
        else:
            cmd = [self.path_to_pbs_scripts+'/nvidia_start_gpu_monitoring','-j', job_id, '-g', json.dumps(nvidia_devices), '-w', str(walltime_secs)]
        
            pbs.logmsg(pbs.EVENT_DEBUG, "cmd=%s" % (cmd))
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, err = process.communicate()
            pbs.logmsg(pbs.EVENT_DEBUG, "output=%s" % (output.strip()))
            pbs.logmsg(pbs.EVENT_DEBUG, "err=%s" % (err))
            j.Variable_List['nvidia_group_id'] = output.strip()

        # Write a file with jobid.cleanup as extension that will contain data [groupId,False]
        self.add_cleanup_entry(job_id,j.Variable_List['nvidia_group_id'])

    
    def get_resources_used(self):
        e = pbs.event()
        j = e.job
        job_id = j.id

        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))
        
        nvidia_devices = self.get_nvidia_devices(job_id)

        if not using_supported_pbs:
            #handleObj, groupObj = self.setup_dcgm_group(job_id, nvidia_devices)
            handleObj, groupObj = self.get_dcgm_group(j.Variable_List['nvidia_group_id'])
        
            # Notify DCGM to stop collecting stats
            groupObj.stats.StopJobStats(job_id)
            
            # get job stats
            jobInfo = groupObj.stats.GetJobStats(job_id)
        
            jobInfoDict = self.convert_dcgmJobInfo_v1(jobInfo)
            
            #pbs.logmsg(pbs.EVENT_DEBUG, "jobInfoDict %s" % (jobInfoDict))
            gpus_info = []
            for gpu in range(0, jobInfoDict['numGpus']):
                #pbs.logjobmsg(pbs.event().job.id, 'jobInfo.gpus[gpu] %s' % (jobInfo.gpus[gpu]))
                gpuInfoDict = self.convert_dcgmGpuUsageInfo_t(jobInfoDict['gpus'][gpu])
    
                #pbs.logjobmsg(pbs.event().job.id, 'gpuInfoDict %s' % (gpuInfoDict))
                
                gpu_str = 'gpu%s:' % gpuInfoDict['gpuId']
                gpu_res = []
                
                for res in self.resource_config['resources'].keys():
                    if res in gpuInfoDict.keys():
                        if isinstance(gpuInfoDict[res], dict):
                            for sub_res in gpuInfoDict[res]:
                                gpu_res.append('%s_%s=%s' % (res, sub_res, gpuInfoDict[res][sub_res]))
                        else:
                            if res == "energyConsumed":
                               gpu_res.append('%s=%s' % (res, round((gpuInfoDict[res]/1000)/((int(gpuInfoDict['endTime']) - int(gpuInfoDict['startTime']))/1000000.0)),2))
                            else:
                               gpu_res.append('%s=%s' % (res, gpuInfoDict[res]))
                
                # Handle special cases
                if 'duration' in self.resource_config['resources'].keys():
                    if 'startTime' in gpuInfoDict.keys() and 'endTime' in gpuInfoDict.keys():
                        gpu_res.append('duration=%.2f' % ((int(gpuInfoDict['endTime']) - int(gpuInfoDict['startTime']))/1000000.0))
                    
                gpu_str += ':'.join(gpu_res)
                
                #pbs.logjobmsg(pbs.event().job.id, 'gpu_str %s' % (gpu_str))
                gpus_info.append(gpu_str)
            
            self.resource_string = '%s:(%s)' % (pbs.get_local_nodename(), '+'.join(gpus_info))
        
            self.delete_group(handleObj, groupObj)
        else:
            cmd = [self.path_to_pbs_scripts+'/nvidia_get_resources_used','-j',job_id, '-G', j.Variable_List['nvidia_group_id'], '-r', json.dumps(self.resource_config['resources'].keys())]
        
            pbs.logmsg(pbs.EVENT_DEBUG, "cmd=%s" % (cmd))
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            output, err = process.communicate()
            pbs.logmsg(pbs.EVENT_DEBUG, "output=%s" % (output.strip()))
            pbs.logmsg(pbs.EVENT_DEBUG, "err=%s" % (err))
            
            resources = json.loads(output)
            
            self.resource_string = '%s:(%s)' % (pbs.get_local_nodename(), resources)

        # Remove .cleanup file for this job since group is destroyed
        self.delete_cleanup_entry(job_id)


    # Make a list of group ids that need to be cleaned! Then iterate over them and delete group

    def cleanup_check(self):
        # Clean up orphan dcgm groups by looking at clean up files with flag=True
        try:
            import glob
    
            cleanup_files = glob.glob(hook_storage_path+'*.cleanup')
    
            if len(cleanup_files) != 0:
                group_ids = []

                for filename in cleanup_files:
                    f = open(filename, "r")
                    line = json.loads(f.readline())
                    f.close()

                    if line[1] == True:
                        group_ids.append(str(line[0]))
                        os.remove(filename)

                pbs.logmsg(pbs.LOG_DEBUG, "group ids to be cleaned %s" % (group_ids))
        
                if len(group_ids) != 0:
                    if not using_supported_pbs:
                        for group_id in group_ids:
                            # May run slow as each time a handle is created, not so in supported pbs
                            handleObj, groupObj = self.get_dcgm_group(group_id)
                            self.delete_group(handleObj, groupObj)
                    else:
                        cmd = [self.path_to_pbs_scripts+'/nvidia_cleanup_groups', '-g', json.dumps(group_ids)]
        
                        pbs.logmsg(pbs.EVENT_DEBUG, "cmd=%s" % (cmd))
                        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        output, err = process.communicate()
                        pbs.logmsg(pbs.EVENT_DEBUG, "output=%s" % (output.strip()))
                        pbs.logmsg(pbs.EVENT_DEBUG, "err=%s" % (err))
    
        except:
            import traceback
            pbs.logmsg(pbs.LOG_WARNING, "Error while cleaning up orphan groups.")
            lines = traceback.format_exc()
            for line in lines.split('\n'):
                pbs.logmsg(pbs.LOG_DEBUG,line)
            pass


    def add_missing_node_resources(self, missing_sister_nodes, node_resources):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

        # Create dummy resource data for missing sister nodes        

        #self_aggregate_resource_string = 'tr-ebu01:(gpu0:startTime=1464884367128380:smClock_average=648:smClock_maxValue=836:smClock_minValue=324:energyConsumed=1099631:maxGpuMemoryUsed=97648640:smUtilization_average=38:smUtilization_maxValue=99:smUtilization_minValue=0:memoryClock_average=1833:memoryClock_maxValue=2505:memoryClock_minValue=324:endTime=1464884379703126:memoryUtilization_average=0:memoryUtilization_maxValue=0:memoryUtilization_minValue=0:duration=12.57+gpu1:startTime=1464884367128380:smClock_average=539:smClock_maxValue=758:smClock_minValue=324:energyConsumed=1010305:maxGpuMemoryUsed=97648640:smUtilization_average=40:smUtilization_maxValue=100:smUtilization_minValue=0:memoryClock_average=1498:memoryClock_maxValue=2505:memoryClock_minValue=324:endTime=1464884379703126:memoryUtilization_average=0:memoryUtilization_maxValue=0:memoryUtilization_minValue=0:duration=12.57)'
    
        # Extract resources and values from first node in self.aggregate_resource_string
        hostname, resource_info = self.aggregate_resource_string.split('+')[0].split(':', 1)
        
        resource_info = resource_info.replace('(', '')
        resource_info = resource_info.replace(')', '')
        
        resources = []
        # Get resources from first GPU 
        for gpu_resource_info in resource_info.split('+'):
            resource_specs = gpu_resource_info.split(':')
            for resource_spec in resource_specs[1:]:
                res, val = resource_spec.split('=')
                resources.append(res)
            break
        
        # Reconstruct resources with * as value
        dummy_resource_value_string = ':'.join(['%s=%s' % (res, self.missing_value_string) for res in resources])
        
    
        nodes_missing_resource_strings = []
        
        for node in missing_sister_nodes:
            gpus_missing_resource_strings = []
            for ngpu in range(0, int(node_resources[node][self.resource])):
                gpus_missing_resource_strings.append('gpu%d:%s' % (ngpu, dummy_resource_value_string))
            nodes_missing_resource_strings.append('%s:(%s)' % (node, '+'.join(gpus_missing_resource_strings)))
        
        missing_resource_string = '+'.join(nodes_missing_resource_strings)
        
        # Add missing_resource_string to self.aggregate_resource_string
        self.aggregate_resource_string = '%s+%s' % (self.aggregate_resource_string, missing_resource_string)
        
        pbs.logmsg(pbs.EVENT_DEBUG, "self.aggregate_resource_string:%s" % (self.aggregate_resource_string))


    def get_resource_vals(self):
        def convert_gpu_time_to_date(gpu_time):
            # https://docs.python.org/2/library/time.html
            t = time.strftime("%a %b %d %H:%M:%S %Y", time.localtime(gpu_time / 1000000.0))    

            return t

        resource_vals = {}
        node_names = []
        
        for node_vals in self.aggregate_resource_string.split(')+'):
            node_name, device_resources = node_vals.split(':', 1)
            device_resources = device_resources.replace('(', '')
            device_resources = device_resources.replace(')', '')
    
            node_names.append(node_name)
            
            for device in device_resources.split('+'):
                device_name, resources = device.split(':', 1)
                for resource in resources.split(':'):
                    res, val = resource.split('=')
                    if res not in resource_vals.keys():
                        resource_vals[res] = {}
                    if node_name not in resource_vals[res].keys():
                        resource_vals[res][node_name] = []
                    
                    if 'Time' in res and val != self.missing_value_string:
                        val = convert_gpu_time_to_date(int(val))
                    
                    resource_vals[res][node_name].append('%s:%s' % (device_name, val))
    
        return resource_vals, node_names


    def get_base_resource(self, resource):
        for res in self.resource_config['resources'].keys():
            if resource.startswith(res):
                return res, resource.replace(res, '')

                
    def set_resources_used(self):
        pbs.logmsg(pbs.EVENT_DEBUG, "In %s:%s" % (self.__class__.__name__, caller_name()))

        e = pbs.event()
        j = e.job
    
        if self.aggregate_resource_string:
            base_resource = {}
            
            # Set per node GPU resources
            resource_vals, node_names = self.get_resource_vals()
            #pbs.logmsg(pbs.EVENT_DEBUG, "resource_vals:%s" % (resource_vals))
            #pbs.logmsg(pbs.EVENT_DEBUG, "node_names:%s" % (node_names))

            for res in resource_vals.keys():
                base_resource[res] = {}
                base_resource[res]['base_resource'], base_resource[res]['extention'] = self.get_base_resource(res)
                
                if self.resource_config['resources'][base_resource[res]['base_resource']]['report_usage']:
                    node_resources = []
                    for node in node_names:
                        # Add unit to resource value
                        if self.resource_config['resources'][base_resource[res]['base_resource']]['unit'] in ['B', 'secs']:
                                node_resources.append('%s:(%s)' % (node, '+'.join(x.split(':')[0]+':'+round_to_best_unit((x + self.resource_config['resources'][base_resource[res]['base_resource']]['unit']).split(':')[1]) for x in resource_vals[res][node])))
                        else:
                                node_resources.append('%s:(%s)' % (node, '+'.join(x + self.resource_config['resources'][base_resource[res]['base_resource']]['unit'] for x in resource_vals[res][node])))
                    # Build resource name
                    j.resources_used[self.resource_config['resources'][base_resource[res]['base_resource']]['PBS_resource']+base_resource[res]['extention']+'_per_node_gpu'] = str('+'.join(node_resources))

            resource_node_totals = {}
 
            # Set per node resources
            for res in resource_vals.keys():
                if self.resource_config['resources'][base_resource[res]['base_resource']]['aggregate_usage'] and self.resource_config['resources'][base_resource[res]['base_resource']]['report_usage']:
                    resource_node_totals[res] = {}
                    node_resources = []
                    for node in node_names:
                        res_total = 0
                        had_valid_resource = False
                        for device_usage in resource_vals[res][node]:
                            device_name, val = device_usage.split(':')
                            if val != self.missing_value_string:
                                had_valid_resource = True
                                if res != 'duration' and res != 'energyConsumed':
                                    res_total += int(val)
                                else:
                                    if float(val) > res_total:
                                        res_total = float(val)
    
                        if not had_valid_resource:
                            res_total = self.missing_value_string
                            
                        # Add unit to resource value
                        val = '%s%s' % (res_total, self.resource_config['resources'][base_resource[res]['base_resource']]['unit'])
                        if self.resource_config['resources'][base_resource[res]['base_resource']]['unit'] in ['B','secs'] and had_valid_resource:
                            val = round_to_best_unit(val)
                        node_resources.append('%s:(%s)' % (node, val))
                        
                        resource_node_totals[res][node] = res_total
                    
                    # Build resource name
                    j.resources_used[self.resource_config['resources'][base_resource[res]['base_resource']]['PBS_resource']+base_resource[res]['extention']+'_per_node'] = str('+'.join(node_resources))
            
            
            # Set total resources
            for res in resource_vals.keys():
                if self.resource_config['resources'][base_resource[res]['base_resource']]['aggregate_usage'] and self.resource_config['resources'][base_resource[res]['base_resource']]['report_usage']:
                    res_total = 0
                    had_valid_resource = False
                    for node in node_names:
                        if resource_node_totals[res][node] != self.missing_value_string:
                            had_valid_resource = True
                            if res != 'duration':
                                res_total += resource_node_totals[res][node]
                            else:
                                if resource_node_totals[res][node] > res_total:
                                    res_total = resource_node_totals[res][node]
                    
                    if not had_valid_resource:
                        res_total = self.missing_value_string
                            
                    # Build resource name
                    # Add unit to resource value
                    val = '%s%s' % (str(res_total), self.resource_config['resources'][base_resource[res]['base_resource']]['unit'])
                    if self.resource_config['resources'][base_resource[res]['base_resource']]['unit'] in ['B','secs'] and had_valid_resource:
                        val = round_to_best_unit(val)
                    j.resources_used[self.resource_config['resources'][base_resource[res]['base_resource']]['PBS_resource']+base_resource[res]['extention']] = str(val)


    def get_defined_value(self, value):
        v = dcgmvalue.DcgmValue(value)
    
        if isinstance(value, dict):
            return value
        try:
            if (v.IsBlank()):
                return None
            else:
                return value
        except:
            return value
    
    
    def convert_dcgmStatSummaryInt64_t(self, info):
        data = {}
        if self.get_defined_value(info.minValue) != None: data['minValue'] = info.minValue
        if self.get_defined_value(info.maxValue) != None: data['maxValue'] = info.maxValue
        if self.get_defined_value(info.average) != None: data['average'] = info.average
         
        return data
    
    
    def convert_dcgmStatSummaryInt32_t(self, info):
        print 'convert_dcgmStatSummaryInt32_t'
        print 'info', info
        print 'info.minValue', info.minValue
        data = {}
        if self.get_defined_value(info.minValue) != None: data['minValue'] = info.minValue
        if self.get_defined_value(info.maxValue) != None: data['maxValue'] = info.maxValue
        if self.get_defined_value(info.average) != None: data['average'] = info.average
        
        return data
    
    
    def convert_dcgmGpuUsageInfo_t_not_used(self, info):
        data = {}
        if self.get_defined_value(info.minValue) != None: data['minValue'] = info.minValue
        if self.get_defined_value(info.maxValue) != None: data['maxValue'] = info.maxValue
        if self.get_defined_value(info.average) != None: data['average'] = info.average
        
        return data
    
    
    def convert_dcgmJobInfo_v1(self, info):
        data = {}
    
        if self.get_defined_value(info.version) != None: data['version'] = info.version
        if self.get_defined_value(info.numGpus) != None: data['numGpus'] = info.numGpus
        if self.get_defined_value(self.convert_dcgmGpuUsageInfo_t(info.summary)) != None: data['summary'] = self.convert_dcgmGpuUsageInfo_t(info.summary)
        if self.get_defined_value(info.gpus) != None: data['gpus'] = info.gpus
        
        return data
        
        
    def convert_dcgmGpuUsageInfo_t(self, info):
        data = {}
        
        data['gpuId'] = info.gpuId
        if self.get_defined_value(info.energyConsumed) != None: data['energyConsumed'] = info.energyConsumed
        if self.get_defined_value(self.convert_dcgmStatSummaryInt64_t(info.pcieRxBandwidth)) != None: data['pcieRxBandwidth'] = self.convert_dcgmStatSummaryInt64_t(info.pcieRxBandwidth)
        if self.get_defined_value(self.convert_dcgmStatSummaryInt64_t(info.pcieTxBandwidth)) != None: data['pcieTxBandwidth'] = self.convert_dcgmStatSummaryInt64_t(info.pcieTxBandwidth)
        if self.get_defined_value(info.pcieReplays) != None: data['pcieReplays'] = info.pcieReplays
        if self.get_defined_value(info.startTime) != None: data['startTime'] = info.startTime
        if self.get_defined_value(info.endTime) != None: data['endTime'] = info.endTime
        data['smUtilization'] = self.convert_dcgmStatSummaryInt32_t(info.smUtilization)
        data['memoryUtilization'] = self.convert_dcgmStatSummaryInt32_t(info.memoryUtilization)
        if self.get_defined_value(info.eccSingleBit) != None: data['eccSingleBit'] = info.eccSingleBit
        if self.get_defined_value(info.eccDoubleBit) != None: data['eccDoubleBit'] = info.eccDoubleBit
        data['memoryClock'] = self.convert_dcgmStatSummaryInt32_t(info.memoryClock)
        data['smClock'] = self.convert_dcgmStatSummaryInt32_t(info.smClock)
        data['maxGpuMemoryUsed'] = info.maxGpuMemoryUsed
        if self.get_defined_value(info.powerViolationTime) != None: data['powerViolationTime'] = info.powerViolationTime
        if self.get_defined_value(info.thermalViolationTime) != None: data['thermalViolationTime'] = info.thermalViolationTime
        if self.get_defined_value(info.reliabilityViolationTime) != None: data['reliabilityViolationTime'] = info.reliabilityViolationTime
        if self.get_defined_value(info.boardLimitViolationTime) != None: data['boardLimitViolationTime'] = info.boardLimitViolationTime
        if self.get_defined_value(info.lowUtilizationTime) != None: data['lowUtilizationTime'] = info.lowUtilizationTime
        if self.get_defined_value(info.syncBoostTime) != None: data['syncBoostTime'] = info.syncBoostTime
    
        data['numComputePids'] = info.numComputePids
        data['computePids'] = []
        for i in range(0, data['numComputePids']):
            data['computePids'].append(info.computePids[i])
            
        data['numGraphicsPids'] = info.numGraphicsPids
        data['graphicsPids'] = []
        for i in range(0, data['numGraphicsPids']):
            data['graphicsPids'].append(info.graphicsPids[i])
    
        data['numXidCriticalErrors'] = info.numXidCriticalErrors
        data['xidCriticalErrorsTs'] = []
        for i in range(0, data['numXidCriticalErrors']):
            data['xidCriticalErrorsTs'].append(info.xidCriticalErrorsTs[i])
    
        return data
    
    
    def setup_dcgm_group(self, group_name, gpus):
        handleObj = pydcgm.DcgmHandle(ipAddress='127.0.0.1', opMode=dcgm_structs.DCGM_OPERATION_MODE_AUTO)
        
        systemObj = handleObj.GetSystem()
        
        groupObj = systemObj.GetEmptyGroup(group_name)
        
        for gpu in gpus:
            groupObj.AddGpu(int(gpu))
        
        gpuIds = groupObj.GetGpuIds() 
        pbs.logjobmsg(pbs.event().job.id, 'gpuIds %s' % (gpuIds))
        
        return handleObj, groupObj
    
    
    def get_dcgm_group(self, group_id):
        handleObj = pydcgm.DcgmHandle(ipAddress='127.0.0.1', opMode=dcgm_structs.DCGM_OPERATION_MODE_AUTO)
        
        systemObj = handleObj.GetSystem()
        
        groupObj = pydcgm.DcgmGroup(handleObj, groupId = int(group_id))
        
        return handleObj, groupObj
    
    
    def delete_group(self, handleObj, groupObj):
        groupObj.Delete()
        
        ## Destroy the group
        del(groupObj)
        groupObj = None
    
        ## Shut down the host engine
        del(handleObj)
        handleObj = None
    
    
    def add_cleanup_entry(self, job_id, group_id):
        job_id = str(job_id)

        try:
            filename = hook_storage_path+job_id+'.cleanup'
            f = open(filename, "w")
            f.write(json.dumps([group_id,False]))
            f.close()

        except:
            pbs.logmsg(pbs.LOG_WARNING, "Error while writing clean up file for job %s"% (job_id))


    def delete_cleanup_entry(self, job_id):
        job_id = str(job_id)

        try:
            filename = hook_storage_path+job_id+'.cleanup'

            if os.path.isfile(filename):
                os.remove(filename)

        except OSError:
            pbs.logmsg(pbs.LOG_WARNING, "Trying to delete clean up file for job %s but file not found."% (job_id))
        except:
            pbs.logmsg(pbs.LOG_WARNING, "Error while removing clean up file for job %s"% (job_id))


    def read_cgroup_host_job_env_file(self, job_id):
        job_id = str(job_id)
    
        # Read assigned_resources
        hook_storage_dir = pbsHome+'/mom_priv/hooks/hook_data/'
        try:
            filename = hook_storage_dir+job_id
            f = open(filename,"r")
            lines = f.readlines()
            f.close()
    
            return lines
        except:
            return []
    
    
    def get_nvidia_devices_on_host(self):
        nvidia_devices = []
        gpu_type_dict  = {}
            
        # Need to find all devices on node
        cmd = ['/usr/bin/nvidia-smi','-q','-x']
    
        try:
            process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            data,err = process.communicate()
            
            # import the xml library and parse the output
            import xml.etree.ElementTree
        
            # Parse the data            
            root = xml.etree.ElementTree.fromstring(data)
            #pbs.logmsg(pbs.EVENT_DEBUG3,"root.tag: %s" % root.tag)
            for child in root:
                if child.tag == "gpu":
                    number = child.find('minor_number').text
                    nvidia_devices.append(number)
                
                    name = child.find('product_name').text
                    if gpu_type_dict.has_key(name):
                        gpu_type_dict[name].append(number)
                    else:
                        gpu_type_dict[name] = [number]
            pbs.logmsg(pbs.EVENT_DEBUG, "nvidia_devices %s" % (nvidia_devices))
            pbs.logmsg(pbs.EVENT_DEBUG, "gpu_ids_by_type %s" % (gpu_type_dict))
        except:
            pbs.logmsg(pbs.EVENT_DEBUG, "command %s failed" % (cmd))
        
        return nvidia_devices,gpu_type_dict
        

    def get_nvidia_devices(self, job_id):
        nvidia_devices = []
            
        if self.using_cgroups:
            lines = self.read_cgroup_host_job_env_file(job_id)
            
            for line in lines:
                data = json.loads(line)
                pbs.logmsg(pbs.EVENT_DEBUG, "data %s" % (data))
                if 'device_names' in data.keys():
                    nvidia_device = True
                    for device in data['device_names']:
                        if 'nvidia' not in device:
                            nvidia_device = False
                            break
                    
                    # Extract device numbers
                    if nvidia_device:
                        for device in data['device_names']:
                            nvidia_devices.append(device.replace('nvidia', ''))
                        
        else:
            nvidia_devices = self.devices_on_host
            
        pbs.logmsg(pbs.EVENT_DEBUG, "nvidia_devices %s" % (nvidia_devices))
        
        return nvidia_devices

# PBS functions
# -------------

def offline_vnodes(comment):
    e = pbs.event()
    hostname = pbs.get_local_nodename()
    
    now = datetime.datetime.now()
    txt = now.strftime("%m/%d/%Y")+" "+now.strftime("%H:%M:%S ")
    txt = txt+str(comment)

    vnlist = e.vnode_list
    #pbs.logmsg(pbs.LOG_DEBUG, 'hostname=%s' % hostname)
    #pbs.logmsg(pbs.LOG_DEBUG, 'vnlist=%s' % vnlist)
    #pbs.logmsg(pbs.LOG_DEBUG, 'comment=%s' % comment)
    for v in vnlist.keys():
        vnlist[v].state = pbs.ND_OFFLINE
        vnlist[v].comment = txt


# Class lock
# --------------

class lock:

    def __init__(self, path):
        self.path = path
        self.fd = None

    def __enter__(self):
        self.fd = open(self.path, 'w')
        fcntl.flock(self.fd, fcntl.LOCK_EX)

    def __exit__(self, exc, val, traceback):
        if self.fd:
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            self.fd.close()


# Size functions
# --------------

def separate_val_unit(amount):
    val = re.findall(r"[-+]?\d*\.\d+|\d+", amount)[0]
    units = amount.replace(val, '')
    
    return val, units


def convert_size(size, src_units, dst_units):
    # Split value from units
    
    scale_factor = 1024.0
    
    # Scale val into Bytes
    if src_units == 'TB':
        sf1 = scale_factor * scale_factor * scale_factor * scale_factor
    elif src_units == 'GB':
        sf1 = scale_factor * scale_factor * scale_factor
    elif src_units == 'MB':
        sf1 = scale_factor * scale_factor
    elif src_units == 'KB':
        sf1 = scale_factor
    elif src_units == 'B':
        sf1 = 1.0
    else:
        sf1 = 1

    # Scale Bytes into final units
    if dst_units == 'TB':
        sf2 = sf1 / (scale_factor * scale_factor * scale_factor * scale_factor)
    elif dst_units == 'GB':
        sf2 = sf1 / (scale_factor * scale_factor * scale_factor)
    elif dst_units == 'MB':
        sf2 = sf1 / (scale_factor * scale_factor)
    elif dst_units == 'KB':
        sf2 = sf1 / scale_factor
    elif dst_units == 'B':
        sf2 = sf1
    else:
        sf2 = sf1
        
    
    return float(size) * sf2
    return int(math.ceil(float(size) * sf2))


def convert_to_gb(size):
    val, units = separate_val_unit(size)

    return convert_size(val, units, 'GB')
    

def aggregate_scratch(res_str):
    pbs.logmsg(pbs.EVENT_DEBUG, "res_str:%s" % (res_str))
    val_gb = 0.0
    
    for node_scratch in res_str.split('+'):
        match = re.match(".*scratch=(\d+.*)\)", node_scratch)
        if match:
            val = match.groups()[0]
            val_gb += convert_to_gb(val)
        
    return '%.3fGB' % val_gb


def round_to_best_unit_aux(aggregate_val):
    size_units = ['TB', 'GB', 'MB', 'KB', 'B']

    val, units = separate_val_unit(aggregate_val)
    unit_index = size_units.index(units)
    
    val = float(val)

    if val >= 1024:
        if unit_index > 0:
            return round_to_best_unit_aux('%f%s' % (val/1024, size_units[unit_index-1]))
    return str(round(val,2))+units;


def round_to_best_unit(aggregate_val):
    val, units = separate_val_unit(aggregate_val);
    if 'B' in units:
        val_str = round_to_best_unit_aux(aggregate_val)
    elif "secs" in units:
        val_str = round_to_best_unit_sec(aggregate_val);
#    val, units = separate_val_unit(val_str)
    
#    return '%d%s' % (int(math.ceil(float(val) - 0.5)), units)
    return val_str;

def round_to_best_unit_sec(aggregate_val):
     val, units = separate_val_unit(aggregate_val)
     val = float(val)
     if val >= 60:
         min = val/60;
         if min >= 60:
             hr = min/60;
             return str(round(hr,2)) + "hrs"
         else:
             return str(round(min,2)) + "mins"
     else:
         return aggregate_val


# Generic framework
# -----------------

'''

Resource usage location on the PFS
$pfs_job_dir/$resource_results_dir/$job_id/$resource/$node

'''


def decode_list(data):
    rv = []
    for item in data:
        if isinstance(item, unicode):
            item = item.encode('utf-8')
        elif isinstance(item, list):
            item = decode_list(item)
        elif isinstance(item, dict):
            item = decode_dict(item)
        rv.append(item)
    return rv


def decode_dict(data):
    rv = {}
    for key, value in data.iteritems():
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        elif isinstance(value, list):
            value = decode_list(value)
        elif isinstance(value, dict):
            value = decode_dict(value)
        rv[key] = value
    return rv


def parse_config_file():
    config = {}
    
    if 'PBS_HOOK_CONFIG_FILE' in os.environ:
        config_file = os.environ["PBS_HOOK_CONFIG_FILE"]
        #pbs.logmsg(pbs.EVENT_DEBUG, "%s: Config file is %s" % (caller_name(), config_file))
        config = json.load(open(config_file, 'r'), object_hook=decode_dict)

    return config


def caller_name():
    return str(sys._getframe(1).f_code.co_name)


def get_resource_nodes(resources, exec_vnode, select):
    resource_nodes = {}
    node_resources = {}
    
    exec_vnode = exec_vnode.replace('(', '')
    exec_vnode = exec_vnode.replace(')', '')
    
    for vnode_spec in exec_vnode.split('+'):
        data = vnode_spec.split(':')
        # Get natural node name, ignore [#]
        vnode_name = data[0].split('[')[0]

        if vnode_name not in node_resources.keys():
            node_resources[vnode_name] = {}
            
        for resource_req in data[1:]:
            res, val = resource_req.split('=')
            if res in resources:
                if res not in node_resources[vnode_name].keys():
                    try:
                        if isinstance(float(val), float):
                            node_resources[vnode_name][res] = 0.0
                    except:
                        node_resources[vnode_name][res] = []
                
                try:
                    if isinstance(float(val), float):
                        node_resources[vnode_name][res] += float(val)
                except:
                    if val not in node_resources[vnode_name][res]:
                        node_resources[vnode_name][res].append(val)
    
        for res in node_resources[vnode_name].keys():
            if res not in resource_nodes.keys():
                resource_nodes[res] = []
            
            if vnode_name not in resource_nodes[res]:
                resource_nodes[res].append(vnode_name)
    
    
    for chunk in select.split('+'):
        nchunks = 1
        found_res = False
        for c in chunk.split(':'):
            kv = c.split('=')
            if len(kv) == 2:
                res = kv[0]
                if res in resources:
                    for vnode_name in node_resources.keys():
                        val = node_resources[vnode_name][res]
                        if res not in node_resources[vnode_name].keys():
                            try:
                                if isinstance(float(val), float):
                                    node_resources[vnode_name][res] = 0.0
                            except:
                                node_resources[vnode_name][res] = []
                        
                        try:
                            if isinstance(float(val), float):
                                node_resources[vnode_name][res] += float(val)
                        except:
                            if val not in node_resources[vnode_name][res]:
                                node_resources[vnode_name][res].append(val)

    for vnode_name in node_resources.keys():
        for res in node_resources[vnode_name].keys():
            if res not in resource_nodes.keys():
                resource_nodes[res] = []
            
            if pbs.get_local_nodename() not in resource_nodes[res]:
                resource_nodes[res].append(vnode_name)
    
    return resource_nodes, node_resources


def save_job_resource_results(job_id, local_nodename, username, resource, resource_str, pfs_job_dir):
    # Save location of resource results file for each job
    if resource_str:
        if not os.path.exists(hook_storage_path):
            try:
                os.makedirs(hook_storage_path)
            except OSError:
                pbs.logmsg(pbs.LOG_DEBUG, 'save_job_resource_results:hook_storage_path %s making error' % (hook_storage_path))
                pass
    
        # Add to local data file
        results_file = os.path.join(hook_storage_path, job_resource_results_file)
        pbs.logmsg(pbs.LOG_DEBUG, 'save_job_resource_results:results_file %s' % (results_file))
        f = open(results_file, 'a')
        f.write('%s,%s,%s\n' % (job_id, username, resource))
        f.close()
        
        # Save in PFS
        res_dir = os.path.join(pfs_job_dir, job_id, resource)
        res_file = os.path.join(res_dir, local_nodename)
        pbs.logmsg(pbs.LOG_DEBUG, 'res_file %s' % (res_file))
        
        if not os.path.exists(res_dir):
            try:
                os.makedirs(res_dir)
            except OSError:
                pass
    
        f = open(res_file, 'w')
        f.write('%s\n' % (resource_str))
        f.close()
    

def save_resource_results(lines):
    if not os.path.exists(hook_storage_path):
        try:
            os.makedirs(hook_storage_path)
        except OSError:
            pbs.logmsg(pbs.LOG_DEBUG, 'save_resource_results:hook_storage_path %s making error' % (hook_storage_path))
            pass

    # Save local data file
    results_file = os.path.join(hook_storage_path, job_resource_results_file)
    pbs.logmsg(pbs.LOG_DEBUG, 'save_resource_results:results_file %s' % (results_file))
    f = open(results_file, 'w')
    for line in lines:
        f.write('%s\n' % line)
    f.close()

    
def get_resource_results():
    # Read local data file
    data = []

    results_file = os.path.join(hook_storage_path, job_resource_results_file)
    if os.path.exists(results_file):
        pbs.logmsg(pbs.LOG_DEBUG, 'results_file %s' % (results_file))
        f = open(results_file, 'r')
        lines = f.readlines()
        f.close()
        
        for line in lines:
            data.append(line.strip())
        
    return data


def delete_file(filename):
    pbs.logmsg(pbs.LOG_DEBUG, 'delete_file %s' % (filename))
    if os.path.exists(filename):
        os.remove(filename)


def delete_results(res_dir, results_files):
    for res_file in sorted(results_files):
        delete_file(os.path.join(res_dir, res_file))


def modify_pfs_job_dir(pfs_job_dir, username):
    pfs_job_dir = pfs_job_dir.replace('$USER', username)

    return pfs_job_dir


def build_resource_object_dict(resource_objects):
    resource_object_dict = {}
    
    for resource_object in resource_objects:
        resource_object_dict[resource_object.resource] = resource_object
    
    return resource_object_dict


def exclude_host(config):
    exclude_host = False
    hostname = pbs.get_local_nodename()

    if config['run_only_on_hosts']:
        if hostname in config['run_only_on_hosts']:
            exclude_host = False
        else:
            exclude_host = True
    else:
        if config['exclude_hosts']:
            if hostname in config['exclude_hosts']:
                exclude_host = True
            else:
                exclude_host = False

    return exclude_host


# Hook functions
# --------------

def exechost_periodic(resource_objects):
    pbs.logmsg(pbs.LOG_DEBUG, 'In %s' % (caller_name()))

    with lock(nvidia_dcgm_lock_file):
        config = parse_config_file()
        
        resource_object_dict = build_resource_object_dict(resource_objects)
    
        # Start resource health checks
        for res in resource_object_dict.keys():
            if resource_object_dict[res].resource_config['run_health_check']:
                resource_object_dict[res].health_check()
                resource_object_dict[res].cleanup_check()
    
        # Clean up orphaned resource results files for this node
        lines = get_resource_results()
        pbs.logmsg(pbs.LOG_DEBUG, 'lines %s' % (lines))
        
        '''
        Find resource results for this node
        if job not running on node
            save job id or increment counter for how many cycles it has been seen
            if number of cycles seen > config value then delete results file
        '''
    
        # Test if resource usage files still exist
        new_lines = []
        for job in lines:
            data = job.split(',')
            # job_id, username, resource, count
        
            pbs_job_dir = modify_pfs_job_dir(config['pfs_job_dir'], data[1])
            
            res_file = os.path.join(pbs_job_dir, data[0], data[2], pbs.get_local_nodename())
            pbs.logmsg(pbs.LOG_DEBUG, 'res_file %s' % (res_file))
            
            if os.path.exists(res_file):
                try:
                    data[3] = str(int(data[3]) + 1)
                except:
                    data.append('1')
    
                # Test if exceeded number of cycles to wait
                if int(data[3]) > config['num_period_cycles_before_clean']:
                    pbs.logmsg(pbs.LOG_DEBUG, 'Exceeded num_period_cycles_before_clean %s deleting res_file %s' % (config['num_period_cycles_before_clean'], res_file))
                    delete_file(res_file)
                else:
                    new_lines.append(','.join(data))
                
        pbs.logmsg(pbs.LOG_DEBUG, 'new_lines %s' % (new_lines))
        save_resource_results(new_lines)
        
        # Test if have empty job directories
        pfs_dir = config['pfs_job_dir']
        
        if '$USER' in pfs_dir:
            have_user_dirs = True
            pre_user_path, post_user_path = pfs_dir.split('$USER')
            post_user_path = re.sub('^/', '', post_user_path)
        else:
            have_user_dirs = False
        
        # Build list of storage dirs
        if have_user_dirs:        
            storage_dirs = [os.path.join(pre_user_path,o,post_user_path) for o in os.listdir(pre_user_path) if os.path.isdir(os.path.join(pre_user_path,o))]
        else:
            storage_dirs = [pfs_dir]
        pbs.logmsg(pbs.LOG_DEBUG, 'storage_dirs %s' % (storage_dirs))
        
        for storage_dir in storage_dirs:
            if os.path.isdir(storage_dir):
                job_dirs = [os.path.join(storage_dir,o) for o in os.listdir(storage_dir) if os.path.isdir(os.path.join(storage_dir,o))]
                pbs.logmsg(pbs.LOG_DEBUG, 'job_dirs %s' % (job_dirs))
                
                for job_dir in job_dirs:
                    res_dirs = [os.path.join(job_dir,o) for o in os.listdir(job_dir) if os.path.isdir(os.path.join(job_dir,o))]
                    pbs.logmsg(pbs.LOG_DEBUG, 'res_dirs %s' % (res_dirs))
                    
                    for res_dir in res_dirs:
                        if not os.listdir(res_dir):
                            pbs.logmsg(pbs.LOG_DEBUG, 'Delete empty resource directory %s' % (res_dir))
                            try:
                                shutil.rmtree(res_dir)
                            except:
                                pass
    
                    if not os.listdir(job_dir):
                        pbs.logmsg(pbs.LOG_DEBUG, 'Delete empty job directory %s' % (job_dir))
                        try:
                            shutil.rmtree(job_dir)
                        except:
                            pass


def execjob_begin(resource_objects):
    pbs.logmsg(pbs.LOG_DEBUG, 'In %s' % (caller_name()))

    with lock(nvidia_dcgm_lock_file):
        e = pbs.event()
        j = e.job
        job_id = j.id
        job_owner = j.Job_Owner.split('@')[0]
        js = pbs.server().job(j.id)
        
        config = parse_config_file()
        
        resource_object_dict = build_resource_object_dict(resource_objects)
    
        # Find the nodes for each resource
        resource_nodes, node_resources = get_resource_nodes(config['resources'].keys(), str(j.exec_vnode), str(js.Resource_List['select']))
    
        # Find the resources used on this node
        # Start resource health and diagnostic checks
        if pbs.get_local_nodename() in node_resources.keys():
            for res in node_resources[pbs.get_local_nodename()]:
                if res in resource_object_dict.keys():
                    if resource_object_dict[res].resource_config['run_health_check']:
                        resource_object_dict[res].health_check()
                        
                    if resource_object_dict[res].resource_config['run_diagnostic_check']:
                        resource_object_dict[res].diagnostic_check()
    
    
        # Find the resources used on this node
        # Start resource monitoring
        if pbs.get_local_nodename() in node_resources.keys():
            for res in node_resources[pbs.get_local_nodename()].keys():
                if res in resource_object_dict.keys():
                    if 'resources' in resource_object_dict[res].resource_config.keys():
                        resource_object_dict[res].start_resource_monitoring()
            

def execjob_epilogue(resource_objects):
    pbs.logmsg(pbs.LOG_DEBUG, 'In %s' % (caller_name()))

    with lock(nvidia_dcgm_lock_file):
        e = pbs.event()
        j = e.job
        job_id = j.id
        job_owner = j.Job_Owner.split('@')[0]
        js = pbs.server().job(j.id)
    
    
        config = parse_config_file()
    
        resource_object_dict = build_resource_object_dict(resource_objects)
    
        # Find the nodes for each resource
        resource_nodes, node_resources = get_resource_nodes(config['resources'].keys(), str(j.exec_vnode), str(js.Resource_List['select']))
    
        pbs.logmsg(pbs.LOG_DEBUG, 'resource_nodes %s' % (resource_nodes))
        pbs.logmsg(pbs.LOG_DEBUG, 'node_resources %s' % (node_resources))
        pbs.logmsg(pbs.LOG_DEBUG, 'resource_object_dict %s' % (resource_object_dict))
    
        # Find the resources used on this node
        # Get resources used
        if pbs.get_local_nodename() in node_resources.keys():
            for res in node_resources[pbs.get_local_nodename()].keys():
                if res in resource_object_dict.keys():
                    if 'resources' in resource_object_dict[res].resource_config.keys():
                        resource_object_dict[res].get_resources_used()
                    
                        pbs.logmsg(pbs.LOG_DEBUG, '%s:resource_string %s' % (res, resource_object_dict[res].resource_string))
            
        pbs_job_dir = modify_pfs_job_dir(config['pfs_job_dir'], job_owner)
            
        # If mother superior collect results from sister nodes and delete all
        # else write results to pfs
        # Test if hook is running on Mother Superior
        if j.in_ms_mom():
            pbs.logmsg(pbs.LOG_DEBUG, 'Running on Mother Superior')
            
            # Need to aggregate each resource and test if all results are back
            
            # Get sister nodes for each resource
            sister_resource_nodes = copy.deepcopy(resource_nodes)
            for res in sister_resource_nodes:
                if pbs.get_local_nodename() in sister_resource_nodes[res]:
                    sister_resource_nodes[res].remove(pbs.get_local_nodename())
            
            pbs.logmsg(pbs.LOG_DEBUG, "sister_resource_nodes %s" % sister_resource_nodes)
            pbs.logmsg(pbs.LOG_DEBUG, "pbs_job_dir %s" % pbs_job_dir)
    
            # Wait for sister nodes to write resource results files
            delay = 0
            missing_sister_nodes = {}
            while True:
                waiting_for_results = False
                
                for res in sister_resource_nodes.keys():
                    if res in resource_object_dict.keys() and 'resources' in resource_object_dict[res].resource_config.keys():
                        missing_sister_nodes[res] = []
                        
                        if sister_resource_nodes[res]:
                            # Have sister nodes
                            res_dir = os.path.join(pbs_job_dir, job_id, res)
                            pbs.logmsg(pbs.LOG_DEBUG, "res_dir %s" % res_dir)
        
                            if os.path.isdir(res_dir):
                                results_files = [o for o in os.listdir(res_dir) if os.path.isfile(os.path.join(res_dir,o))]
                                pbs.logmsg(pbs.LOG_DEBUG, "%s results_files %s" % (res, results_files))
                                
                                # Test if have results from all sister nodes
                                if len(results_files) < len(sister_resource_nodes[res]):
                                    waiting_for_results = True
                            else:
                                results_files = []
                                waiting_for_results = True
                        
                    if delay >= int(config['num_secs_to_wait_for_sisters']):
                        missing_sister_nodes[res] = list(set(sister_resource_nodes[res]) - set(results_files))
                        waiting_for_results = False
                
                if not waiting_for_results:
                    break
                
                delay += 1
                time.sleep(1)
    
            # Aggregate results for each resource
            for res in sister_resource_nodes.keys():
                if res in resource_object_dict.keys():
                    if 'resources' in resource_object_dict[res].resource_config.keys():
                        resource_object_dict[res].set_mother_superior_node_resources()
    
                        if sister_resource_nodes[res]:
                            # Have sister nodes
                            res_dir = os.path.join(pbs_job_dir, job_id, res)
    
                            if os.path.isdir(res_dir):
                                results_files = [o for o in os.listdir(res_dir) if os.path.isfile(os.path.join(res_dir,o))]
                
                                pbs.logmsg(pbs.LOG_DEBUG, "%s aggregate results_files %s" % (res, results_files))
                                
                                resource_object_dict[res].aggregate_resources(res_dir, results_files)
                                
                                delete_results(res_dir, results_files)
    
            # Find if any sister nodes failed to report results
            pbs.logmsg(pbs.LOG_DEBUG, 'missing_sister_nodes %s' % (missing_sister_nodes))
            pbs.logmsg(pbs.LOG_DEBUG, 'node_resources %s' % (node_resources))
    
            '''
            # Simulate a missing node
            node_resources['node1'] = node_resources[pbs.get_local_nodename()]
            missing_sister_nodes['ngpus'].append('node1')
            pbs.logmsg(pbs.LOG_DEBUG, 'missing_sister_nodes %s' % (missing_sister_nodes))
            pbs.logmsg(pbs.LOG_DEBUG, 'node_resources %s' % (node_resources))
            '''
            
            missing_resource_string = {}
            for res in missing_sister_nodes.keys():
                if missing_sister_nodes[res]:
                    pbs.logmsg(pbs.LOG_DEBUG, 'Error: resource %s did not get resoursce usage from nodes %s' % (res, ','.join(missing_sister_nodes[res])))
                    resource_object_dict[res].add_missing_node_resources(missing_sister_nodes[res], node_resources)
                    
            
            # Set resources used
            for res in sister_resource_nodes.keys():
                if res in resource_object_dict.keys():
                    if 'resources' in resource_object_dict[res].resource_config.keys():
                        resource_object_dict[res].set_resources_used()
        else:
            pbs.logmsg(pbs.LOG_DEBUG, 'Running on Sister Node')
    
            for res in resource_object_dict.keys():
                if 'resources' in resource_object_dict[res].resource_config.keys():
                    save_job_resource_results(job_id, pbs.get_local_nodename(), job_owner, res, resource_object_dict[res].resource_string, pbs_job_dir)
    
    
        # Find the resources used on this node
        # Start resource diagnostic checks
        if pbs.get_local_nodename() in node_resources.keys():
            for res in node_resources[pbs.get_local_nodename()].keys():
                if res in resource_object_dict.keys():
                    if resource_object_dict[res].resource_config['run_diagnostic_check']:
                        resource_object_dict[res].diagnostic_check()


# (qdel force) may lead to race condition where end hook may run before or simultaneously with epilogue hook.
# lock is required to make them run exclusively
def execjob_end(resource_objects):
    pbs.logmsg(pbs.LOG_DEBUG, 'In %s' % (caller_name()))

    with lock(nvidia_dcgm_lock_file):
        e = pbs.event()
        j = e.job
        job_id = j.id
    
        try:
            filename = hook_storage_path+job_id+'.cleanup'
    
            if os.path.isfile(filename):
                f = open(filename, "r")
                line = json.loads(f.readline())
                f.close()
    
                group_id = str(line[0])
    
                # Mark job's group to be cleaned up
                # Important to over write the file because if less data is written than previous write
    
                f = open(filename, "w+")
                f.write(json.dumps([group_id,True]))
                f.close()
    
        except:
            pbs.logmsg(pbs.LOG_WARNING, "Error while looking for clean up file %s, it might be left behind. Clean up manually."% (job_id+'.cleanup'))
            pass


try:
    pbs.logmsg(pbs.LOG_DEBUG, 'Starting %s' % (pbs.event().hook_name))

    resource_objects = [ngpus()]
    
    if pbs.event().type == pbs.EXECJOB_BEGIN:
        execjob_begin(resource_objects)
    #if pbs.event().type == pbs.EXECJOB_PROLOGUE:
    #    execjob_begin(resource_objects)
    if pbs.event().type == pbs.EXECJOB_EPILOGUE:
        execjob_epilogue(resource_objects)
    if pbs.event().type == pbs.EXECJOB_END:
        execjob_end(resource_objects)
    if pbs.event().type == pbs.EXECHOST_PERIODIC:
        exechost_periodic(resource_objects)


    pbs.event().accept()


except SystemExit:
    pass


except :
    import traceback
    
    log_buffer = traceback.format_exc()
    pbs.logmsg(pbs.LOG_DEBUG, 'Hook exception:')
    for line in log_buffer.split('\n'):
        pbs.logmsg(pbs.LOG_DEBUG, line)
    pbs.event().reject("Exception trapped in %s:\n %s" % (pbs.event().hook_name, log_buffer))


