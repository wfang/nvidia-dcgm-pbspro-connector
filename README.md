Copyright (C) 1994-2019 Altair Engineering, Inc.
For more information, contact Altair at www.altair.com.

This file is part of the PBS Professional ("PBS Pro") software.

Open Source License Information:

PBS Pro is free software. You can redistribute it and/or modify it under the
terms of the GNU Affero General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

PBS Pro is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.
See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Commercial License Information:

For a copy of the commercial license terms and conditions,
go to: (http://www.pbspro.com/UserArea/agreement.html)
or contact the Altair Legal Department.

Altair’s dual-license business model allows companies, individuals, and
organizations to create proprietary derivative works of PBS Pro and
distribute them - whether embedded or bundled with other software -
under a commercial license agreement.

Use of Altair’s trademarks, including but not limited to "PBS™",
"PBS Professional®", and "PBS Pro™" and Altair’s logos is subject to Altair's
trademark licensing policies.

# PBSPro NVIDIA DCGM Integration Guide #

## PBSPro with NVIDIA Data Center GPU Management (DCGM) Integration Package ##

### Description ###
PBS Professional (PBS Pro) software optimizes job scheduling and workload management in high-performance computing (HPC) and artificial intelligent (AI) environments – clusters, clouds, and supercomputers – improving system efficiency and people’s productivity.

NVIDIA DCGM is a suite of tools for managing and monitoring GPUs in cluster environments. It includes active health monitoring, comprehensive diagnostics, system alerts and governance policies including power and clock management.

PBSPro DCGM Integration allows a site running PBS Pro workload manager be able to run diagnostics on its NVIDIA GPUs, monitor health of NVIDIA GPUs, monitor NVIDIA GPU utilization by jobs.

## Using the PBSPro NVIDIA DCGM Integration ##

The Integration project installs a PBS hook called 'resource_framework_hook' which runs when job is run, job ends or periodically. In each instance of run, it performs certain tasks related to setting up DCGM group for GPU resource monitoring, clean up activities or performing health checks or diagnostics. The node is turned offline with a comment if health check or diagnostic fails. To assign GPUs to the jobs and pinning them to the job, PBS Pro uses another hook called 'PBS_cgroups'. Integration project's hook, 'resource_framework_hook' makes use of the information that PBS_cgroups creates. If for some reason, GPU allocation to the job fails, its likely that resource_framework_hook will fail too. If PBS_cgroups hook is not used then all GPUs on the node are assigned to the job.

Resource usage information is stored temporarily to shared file system by the hook. In multi-vnoded jobs, resource usage information is fetched by hook running on each node and stored temporarily. This is consolidated by the hook running on Mother Superior MOM.

The hook can be configured to run health check and diagnostic check periodically. If the check fails with error or warning, the node is marked offline. A comment is set on the node which can be viewed using pbsnodes command. 

Let us look at a sample output of resource usage.

> 09/03/2019 23:27:27  A	user=dummy group=dummy project=_pbs_project_default jobname=submitjob queue=workq ctime=1567567580 qtime=1567567580 etime=1567567580
>   start=1567567582 exec_host=tr-ebu01/0 exec_vnode=(tr-ebu01:ncpus=1:ngpus=1:mem=870400kb) Resource_List.mem=850mb
>   Resource_List.ncpus=1 Resource_List.ngpus=1 Resource_List.nodect=1 Resource_List.place=free
>   Resource_List.select=1:ncpus=1:ngpus=1:mem=850mb Resource_List.walltime=00:05:00 session=6115 end=1567567647 Exit_status=0
>   resources_used.cpupercent=25 resources_used.cput=00:00:55 resources_used.GPU_duration=1.07mins
>   resources_used.GPU_duration_per_node=tr-ebu01:(1.07mins) resources_used.GPU_duration_per_node_gpu=tr-ebu01:(gpu0:1.07mins)
>   resources_used.GPU_endTime_per_node_gpu=tr-ebu01:(gpu0:Tue Sep 03 23:27:26 2019) resources_used.GPU_energyConsumed=27.84W
>   resources_used.GPU_energyConsumed_per_node=tr-ebu01:(27.84W) resources_used.GPU_energyConsumed_per_node_gpu=tr-ebu01:(gpu0:27.84W)
>   resources_used.GPU_maxGpuMemoryUsed=0.0B resources_used.GPU_maxGpuMemoryUsed_per_node=tr-ebu01:(0.0B)
>   resources_used.GPU_maxGpuMemoryUsed_per_node_gpu=tr-ebu01:(gpu0:0.0B)
>   resources_used.GPU_memoryClock_average_per_node_gpu=tr-ebu01:(gpu0:324MHz)
>   resources_used.GPU_memoryClock_maxValue_per_node_gpu=tr-ebu01:(gpu0:324MHz)
>   resources_used.GPU_memoryClock_minValue_per_node_gpu=tr-ebu01:(gpu0:324MHz)
>   resources_used.GPU_memoryUtilization_average_per_node_gpu=tr-ebu01:(gpu0:0%)
>   resources_used.GPU_memoryUtilization_maxValue_per_node_gpu=tr-ebu01:(gpu0:0%)
>   resources_used.GPU_memoryUtilization_minValue_per_node_gpu=tr-ebu01:(gpu0:0%)
>   resources_used.GPU_smClock_average_per_node_gpu=tr-ebu01:(gpu0:324MHz)
>   resources_used.GPU_smClock_maxValue_per_node_gpu=tr-ebu01:(gpu0:324MHz)
>   resources_used.GPU_smClock_minValue_per_node_gpu=tr-ebu01:(gpu0:324MHz)
>   resources_used.GPU_smUtilization_average_per_node_gpu=tr-ebu01:(gpu0:0%)
>   resources_used.GPU_smUtilization_maxValue_per_node_gpu=tr-ebu01:(gpu0:0%)
>   resources_used.GPU_smUtilization_minValue_per_node_gpu=tr-ebu01:(gpu0:0%)
>   resources_used.GPU_startTime_per_node_gpu=tr-ebu01:(gpu0:Tue Sep 03 23:26:22 2019) resources_used.mem=51428kb resources_used.ncpus=1
>   resources_used.vmem=51428kb resources_used.walltime=00:01:04 run_count=1

Various parameters are captured as mentioned in the config json file of the hook. Resource usage for these parameters can be turned on or off.

## Prerequisites ##

The following prerequisites must be met.

- The Integration package supports PBS version 18.x or higher. So PBS version 18.x or higher must be installed as workload manager on the cluster.
- DCGM v1.3.3 or later is installed.
- datacenter-gpu-manager-375.41-1.x86_64.rpm or higher is installed with all the necessary NVIDIA packages.
- nv-hostengine is running.
- A shared directory location across the compute nodes and PBS server node.

## Installation & Configuration ##

You can download the package from https://github.com/PBSPro/nvidia-dcgm-pbspro-connector and copy it to local directory on PBS server node.

The steps below describe the installation of the package.

1. Unzip the pbs-nvdia package at location: /opt/pbs/nvidia/
1. Navigate to location: /opt/pbs/nvidia/ and give the script executable permission by running below command:

     `chmod +x resource_framework_hook.py PBS_scripts/*`

1. Edit resource_framework_hook.json file.

   a. Search for pfs_job_dir variable and replace global_fs with your shared/global file system which is accessible across all compute nodes.

   b. To enable GPU stats collection, enable cgroups hook use in resource framework hook's configuration file as below:

     `"using_cgroups": true`

1. Copy /opt/pbs/nvidia/PBS_scripts directory to all compute node at location /opt/pbs/nvidia/. Give the scripts executable permission.
1. Add custom resources that hold resource usage values. Run below commands to add the custom resource:

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


1. Now create the hook as described below:

     qmgr -c "c h resource_framework_hook"

     qmgr -c "s h resource_framework_hook enabled=t"

     qmgr -c "s h resource_framework_hook event = 'execjob_begin,execjob_epilogue,execjob_end,exechost_periodic'"

     qmgr -c "i h resource_framework_hook application/x-python default resource_framework_hook.py"

     qmgr -c "i h resource_framework_hook application/x-config default resource_framework_hook.json"

1. Set order of resource_framework_hook higher than that of PBS_cgroups.

   Check order of PBS_cgroups
    
    `qmgr -c "p h PBS_cgroup order"`

   Then set order for resource_framework_hook           

    `qmgr -c "s h resource_framework_hook order=new-value"`
 
1. Add the ngpus to the resources: line in sched_config.

    `resources: "ncpus, mem, arch, host, vnode, netwins, aoe, ngpus"`

1. To enable GPU allocation, enable use of devices in pbs cgroups hook's configuration file as below
 
   `"devices" : {   "enabled"  :  true,`

1. Enable cgroups hook in PBS if not already done. Run below command to enable hook:

    qmgr -c "set hook PBS_cgroups enabled=t"

1. Restart PBS server

Verify if ngpus resource is configured for all the nodes with the help of below command:

  `pbsnodes -av`

If ngpus resource is not configured follow below steps.

  Run below command to configure ngpus.


    qmgr -c "c r ngpus type=long,flag=hn"

  Use the qmgr command to set the value of the ngpus resource on the host:


    qmgr -c "set node Host1 ngpus=<number of GPUs>"

Restart PBS server

Run below command verify the node details:

 `pbsnodes -av`

## Upgrade ##

If you are upgrading or reinstalling PBS Pro NVIDIA DCGM Integration package, you need to remember the following:

1. Restart nv-hostengine. This is required to clear unique ids that dcgmi command keeps in memory. Failing to do so may result in duplicate entries being attempted which will be rejected by dcgmi and hook will fail. PBS creates a group with a unique id, using its job id to group the gpus for utilization of resources. Since job ids start all over again from 0 this may lead to duplicate entries.
 
1. If you forgot to enable PBS_cgroups and are doing it, do restart the PBS service.

## Description of Configuration Parameters ##

#### exclude_hosts ####
To exclude host from running DCGM, add host name in json file. Use double quotes.

  Example:

          "exclude_hosts": ["node01", "node02"]

If both, run_only_on_hosts and exclude_hosts are set, run_only_on_hosts takes precedence.

#### run_only_on_hosts ####

To run DCGM only on specific nodes, add host name in json file. Use double quotes.

  Example:

          "run_only_on_hosts": ["node01"]

If both, run_only_on_hosts and exclude_hosts are set, run_only_on_hosts takes precedence.

#### enable_validation ####

To bypass health check and force validation result. Validation result can be any of validation_mode values.

#### validation_mode ####

Mode can be "Pass", "Warning" or "Error". Used with enable_validation.

#### pfs_job_dir ####

Location to a shared path where resource_framework_hook writes resource information of job.

#### path_to_pbs_scripts ####

Location to a path where scripts are stored.

#### missing_value_string ####

A single character to be used if a value for a resource usage is empty.

#### run_health_check ####

Enable or disable health check performed when job is run or periodically.

#### run_diagnostic_check ####

Enable or disable diagnostic check performed when job is run or periodically.

#### using_cgroups ####

If cgroup hook is being used or not. 'cgroups hook' is used to assign GPUs and pin them to the PBS job. If cgroup hook is not used then all GPUs reported on the node are used.

## Validate the installation ##

After the installation and configuration, you can validate if your setup is complete.

DCGM Validation

Health check: Run below command, ouput should show health status pass: 

/opt/pbs/nvidia/PBS_scripts/nvidia_health_check 

Diagnostic check: Run below command, output should be []: 

/opt/pbs/nvidia/PBS_scripts/nvidia_diagnostic_check 

Integrated setup validation

Submit a job requesting ngpu resource. Below is the example: 

/opt/pbs/default/bin/qsub -j oe -l select=1:ncpus=1:ngpus=1 -- /bin/sleep 5 

Once job is finished, run below command. Check for GPU_ resources in the output.

sudo tracejob <Job Id>

## Troubleshooting ##

MOM logs record the proceedings of the hook. In case of any unexpected behavior, check the mom logs.

- If you see hook timeout exception, you may increase period of execuition as below

   qmgr -c "set hook resource_framework_hook period=120"
- Force deleting a job does not provide accounting. Hence you will not see GPU accounting logs if a job is deleted using -Wforce.
- If you see hook exception check if ^M character is present in scripts in PBS_scripts folder. Run dos2unix command to remove them.
- If you see python interpreter exception check if you have python in your PYTHONPATH or check sys.path. You may also check for /bin/python in your path. The scripts use /bin/python in interpreter line.
- Check if scripts are executable.
- If job is not assigned with GPU check scheduler config for ngpus in resources.
- dcgm keeps unique ids in its memory. If this id is reused, it returns an error. This causes duplicate group error message in the mom logs file.
- you can monitor groups using 'dcgmi group -l' to see if groups are being cleaned up.
- make sure nvidia-smi command is working fine and is in the path /usr/bin