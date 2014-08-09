#!/usr/bin/env python
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Author: Miguel Lucero ( @miguellucero / http://toyelephants.wordpress.com ) 
# 
# yarncalc.py - Script to calculate YARN Memory Configuration
#
# (Values Based on HortonWorks documentation: http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.6.0/bk_installing_manually_book/content/rpm-chap1-11.html)
#
#

import os
import sys
from distutils.util import strtobool

print "\nYARN Memory Calculator"
print "---"
print "(Values are per node)"

# Default Container Memory Size
minContainerSize = 2048

cores = raw_input('Enter number of CORES: ')
ram = raw_input('Enter amount of RAM(GB): ')
disks = raw_input('Enter number of DISKS: ')
hbase = raw_input('Will HBase run on this cluster?(y/n) ')

cores = int(cores)
ram = int(ram)
disks = int(disks)
hbase = strtobool(hbase)

# Logic for large table in Hortonworks Documentation
if ram < 4:
    ram = ram - 1
    if hbase:
        ram = ram -1
elif ram >= 4 and ram < 8:
    ram = ram - 1 # Reserved System Memory
    if hbase:
        ram = ram - 1 # Reserved HBase Memory
elif ram >= 8 and ram < 16:
    ram = ram - 2
    if hbase:
        ram = ram - 1
elif ram >= 16 and ram < 24:
    ram = ram - 2
    if hbase:
        ram = ram - 2
elif ram >= 24 and ram < 48:
    ram = ram - 4
    if hbase:
        ram = ram - 4
elif ram >= 48 and ram < 64:
    ram = ram - 6
    if hbase:
        ram = ram - 8
elif ram >= 64 and ram < 96:
    ram = ram - 8
    if hbase:
        ram = ram - 8
elif ram >= 96 and ram < 128:
    ram = ram - 12
    if hbase:
        ram = ram - 16
elif ram >= 128 and ram < 256:
    ram = ram - 24
    if hbase:
        ram = ram - 24
elif ram >= 256 and ram < 512:
    ram = ram - 32
    if hbase:
        ram = ram - 32
elif ram >= 512:
    ram = ram - 64
    if hbase:
        ram = ram - 64

# Determine minimum container memory size
if ram < 4:
    minContainerSize = 256
elif ram >= 4 and ram < 8:
    minContainerSize = 512
elif ram >= 8 and ram < 24:
    minContainerSize = 1024
elif ram >= 24:
    minContainerSize = 2048

# Determine number of containers per node
containers = min(2*cores,1.8*disks,(ram*1024)/minContainerSize)

# Determine RAM Per Container
ramPerContainer = int(max(minContainerSize,(ram*1024)/containers))

# Generate values for xml properties
yarnNodeManagerResourceMemoryMB = int(containers * ramPerContainer)
yarnSchedulerMinimumAllocationMB = int(ramPerContainer)
yarnSchedulerMaximumAllocationMB = int(containers * ramPerContainer)
mapReduceMapMemoryMB = int(ramPerContainer)
mapReduceReduceMemoryMB = int(2*ramPerContainer)
mapReduceMapJavaOpts = int(0.8*ramPerContainer)
mapReduceReduceJavaOpts = int(0.8*2*ramPerContainer)
yarnAppMapReduceAMResourceMB = int(2*ramPerContainer)
yarnAppMapReduceAMCommandOpts = int(0.8*2*ramPerContainer)

print "\nSuggested YARN Values: "
print "======================="
print "Containers per node:"+str(containers)
print "YARN NodeManager Memory(MB):"+str(yarnNodeManagerResourceMemoryMB)
print "Memory per Container:"+str(ramPerContainer)
print "Mapper Memory MB:"+str(mapReduceMapMemoryMB)
print "Reducer Memory MB:"+str(mapReduceReduceMemoryMB)
print "======================="

print "\nyarn-site.xml properties:"
print "====================================="
print """<property>
<description>Amount of physical memory, in MB, that can be allocated for containers.</description>
<name>yarn.nodemanager.resource.memory-mb</name>
<value>"""+str(yarnNodeManagerResourceMemoryMB)+"""</value>
</property>"""

print """<property>
<description>The minimum allocation size for every container request at the RM, in MBs. Memory requests lower than this won't take effect,
and the specified value will get allocated at minimum.</description>
<name>yarn.scheduler.minimum-allocation-mb</name>
<value>"""+str(yarnSchedulerMinimumAllocationMB)+"""</value>
</property>"""

print """<property>
<description>The maximum allocation size for every container request at the RM, in MBs. Memory requests higher than this won't take effect,
and will get capped to this value.</description>
<name>yarn.scheduler.maximum-allocation-mb</name>
<value>"""+str(yarnSchedulerMaximumAllocationMB)+"""</value>
</property>"""

print """<property>
<name>yarn.app.mapreduce.am.resource.mb</name>
<value>"""+str(yarnAppMapReduceAMResourceMB)+"""</value>
</property>"""

print """<property>
<name>yarn.app.mapreduce.am.command-opts</name>
<value>-Xmx"""+str(yarnAppMapReduceAMCommandOpts)+"""mb</value>
</property>"""

print "======================================="
print "\nmapred-site.xml properties:"
print "======================================="
print """<property>
<name>mapreduce.map.memory.mb</name>
<value>"""+str(mapReduceMapMemoryMB)+"""</value>
</property>"""
print """<property>
<name>mapreduce.reduce.memory.mb</name>
<value>"""+str(mapReduceReduceMemoryMB)+"""</value>
</property>"""
print """<property>
<name>mapreduce.map.java.opts</name>
<value>-Xmx"""+str(mapReduceMapJavaOpts)+"""mb</value>
</property>"""
print """<property>
<name>mapreduce.reduce.java.opts</name>
<value>-Xmx"""+str(mapReduceReduceJavaOpts)+"""mb</value>
</property>"""
print "======================================"
