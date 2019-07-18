#
# Add description of the project here.
#

PBSPro with NVIDIA Data Center GPU Management (DCGM) integration package.

Prerequisite:
-------------
 - PBSPro 13.x onwards 
 - DCGM v1.3.3 or later
 - datacenter-gpu-manager-375.41-1.x86_64.rpm and higher is installed with all the necessary nvidia packages.
 - nv-hostengine  must be running.
 
Ngpus custom resource setup:
---------------------------
 - Check if ngpus resource is configured for all the nodes with the help of below command:
 
```
 	pbsnodes -av
```
 
 - If ngpus resource is not configured follow below steps.
 
 - Run below command to configure ngpus.
 
```
	qmgr -c "c r ngpus type=long,flag=hn"
```
- Add the ngpus to the resources: line in sched_config: 

```
	resources: "ncpus, mem, arch, host, vnode, netwins, aoe, ngpus"
```
- Use the qmgr command to set the value of the ngpus resource on the host:

```
	qmgr -c "set node Host1 ngpus=<number of GPUs>"
```

- Restart PBS server

- Run below command verify the node details:

```
	pbsnodes -av
```


Setup:
------
- Unzip the pbs-nvdia package at location: /opt/pbs/nvidia/

- Navigate to location: /opt/pbs/nvidia/

- Give the script executable permission by running below command:
```
    chmod +x resource_framework_hook.py PBS_scripts/*
```
-	Edit resource_framework_hook.json file.

-	Search for pfs_job_dir variable and replace global_fs with your shared/global file system which is accessible across all compute nodes.

- copy /opt/pbs/nvidia/PBS_scripts directory to all compute node at location /opt/pbs/nvidia/

- No need to add PBS_VERSION directive at the end of file /etc/pbs.conf. Version is detected automatically from file pbs_version.

- Run below command to add the custom resource:
 ```
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
```
- Run below command to configure the hook:
```
    qmgr -c "c h resource_framework_hook"
    qmgr -c "s h resource_framework_hook enabled=t"
    qmgr -c "s h resource_framework_hook event = 'execjob_begin,execjob_epilogue,execjob_end,exechost_periodic'"
    qmgr -c "i h resource_framework_hook application/x-python default resource_framework_hook.py"
    qmgr -c "i h resource_framework_hook application/x-config default resource_framework_hook.json"
```		
- Make following changes if using resource ngpus:

  To enable GPU stats collection, enable cgroups hook use in resource framework hook's configuration file as below

    "using_cgroups": true

  To enable GPU allocation, enable use of devices in pbs cgroups hook's configuration file as below

    "devices" : {
        "enabled"  :  true,
```		
- Setup complete.
 
DCGM Validation:
----------------
- Health check: Run below command, ouput should show health status pass:

```
    /opt/pbs/nvidia/PBS_scripts/nvidia_health_check
```
- Diagnostic check: Run below command, output should be []:

```
    /opt/pbs/nvidia/PBS_scripts/nvidia_diagnostic_check
```
Integrated setup validation:
-----------------------------
 
- Submit any job and request for ngpu resource. Below is the example:
```
    /opt/pbs/default/bin/qsub -j oe -l select=1:ncpus=1:ngpus=1 -- /bin/sleep 5
```
- Once job is finished, Run below command 
```
	qstat -xf <Job Id>
```

 
Package Tested on:
--------------------

- PBSPro 13.1.0
- PBSPro 14.2.4
- PBSPro 18.2.1 (Check notes for special instruction)
- PBSPro 19.2.2
- DCGM v1.3.3
- Early access datacenter-gpu-manager-375.41-1.x86_64.rpm package prodived by NVIDIA.
 
Note:
--------------

- In Diagnostic test, CUDA Toolkit Libraries check is ignored.
- To exclude host from running DCGM, add host name in json file. Use double quotes.
  Example:
          "exclude_hosts": ["node01", "node02"]
- To run DCGM only on specific nodes, add host name in json file. Use double quotes.
  Example:
          "run_only_on_hosts": ["node01"]
- If both, run_only_on_hosts and exclude_hosts are set, run_only_on_hosts takes precedence. 
- You might want to do dos2unix on all scripts if you see ^M error or exception.
- In Health check, validation of test results can be tweaked to determine if scripts can react to errors in health checks. Validation failure can be faked by setting enable_validation to true, by default it is false and validation_mode can be set to either "Warning" (default) or "Error".
- Force deleting a job does not provide accounting. Hence you will not see GPU accounting logs in this case.
- Using PBS Pro version 18.2.x or higher (they have cgroups support inbuilt), you will have to assign higher order to resource_framework_hook than cgroups hook. This version of PBS Pro has built-in cgroups hook.
