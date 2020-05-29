![Logo](https://github.com/Viasat/nhd/blob/master/img/nhd_small.png)

NHD is a topology-aware custom scheduler used for Kubernetes. The scheduler is aware of low-level hardware details, such as CPU count, hyperthreading, GPUs, NUMA nodes, NICs, and more. Using this knowledge, the scheduler can make a better decision than the default Kubernetes scheduler for workload placement. This is especially important for high-throughput and low-latency tasks.

- [Requirements](#requirements)
- [Installing](#installing)
- [Usage](#usage)
- [Design](#design)
  * [Node Filtering](#node-filtering)
     + [CPU Cores](#cpu-cores)  
     + [GPUs](#gpus)     
     + [NICs](#nics)          
  
## Features
* NUMA-aware scheduling for CPUs, GPUs, and NICs
* PCI-aware scheduling for GPU and NICs for GPUDirect applications
* gRPC interface for scheduling statistics
* Node groupings where nodes can be isolated into groups, and pods can map N:N to groups
* SR-IOV support, including all NUMA/PCI scheduling constraints
* Adhere's to normal Kubernetes cordons/uncordon conventions

## Requirements
* Python 3.7 or higher
* libconfig 2.0.0
* magicattr
* Official Kubernetes Python API (https://github.com/kubernetes-client/python)
* Existing kubeconfig in ~/.kube/config
* NFD (Node Feature Discovery) in Kubernetes with Viasat user hook (https://github.com/kubernetes-sigs/node-feature-discovery)
* kopf 0.27rc6 or higher

## Preparing Cluster
NHD requires several things to be in place in your cluster before it can be used.

### Node Taints
NHD requires the following node taint on any node that will be used for scheduling:

```
sigproc.viasat.io/nhd_scheduler=allow:NoSchedule
```

A node can be tainted by runnning:

```
kubectl taint node fi-gcomp011.nae05.v3gdev.viasat.io sigproc.viasat.io/nhd_schedule
```

NHD will scan all nodes at startup, and anything labeled with that node taint will be used by NHD for scheduling.

### Node Feature Discovery
The Node Feature Discovery tool found here: https://github.com/kubernetes-sigs/node-feature-discovery must label the nodes
with resource information for NHD to work properly. NHD also requires a special Viasat user hook, which adds the following 
features:

* Network
     1) Vendor
     2) Interface name
     3) Interface speed
     4) NUMA node
     5) NIC model
     6) Number of VFs
* GPU
     1) Device ID
     2) Device type
     3) NUMA node
* CPU
     1) Number of sockets
     2) Number of cores
     3) Isolcpus boot parameter

NFD can be run as little or as often as the operator chooses, but it must run at least one time on startup.
Once the nodes are labeled, you can choose to run it periodically, or as a one-shot when things need to be re-scanned.

### SR-IOV CNI Plugin
Install the SR-IOV CNI plugin from here https://github.com/hustcat/sriov-cni if using.

### Host-Device Plugin
Install the host-device device plugin from here https://github.com/containernetworking/plugins/tree/master/plugins/main/host-device if using.

## Installing NHD
NHD is deployed as a Docker container into a Kubernetes cluster. All components necessary to deploy the image are in the deploy directory.
Only a single instance of the container is deployed as a replicaset, and if the pod fails, it can be restarted safely. All state
information is stored in the pod specs and configmaps through the Kubernetes API server. This allows NHD to be restarted or upgraded without affecting
any currently-deployed pods.

### Installing From Source
You can build the source and container from typing `make release` from the top level. Note that this will build the Python wheel, Docker container, and 
push to a remote register. You will need to change the string `MY_REPO` at the top of the make file to push this locally.  You will need to select
a base OS image.  Set `OS_FULLPATH` to `ubuntu:18.04` if you don't otherwise have a base OS image preference. 

### Installing From A Public Repo
Coming soon.


## Usage
Before using the scheduler, the pod's yaml and configuration must be updated to work with NHD.

### Scheduler name
To use NHD, the pod spec must contain the following:
```
schedulerName: nhd-scheduler
```
This will instruct the default Kubernetes scheduler to leave the pod alone, and only the NHD scheduler can do any binding operations necessary.

### Configuration
Currently the only configuration type supported by NHD is a Viasat-internal libconfig format. The configuration is mounted as a ConfigMap
into the pod, and translated to the NHD CfgTopology format. 

To make this agnostic to the configuration format, we plan to convert this into a common JSON struct that's consumable by NHD, and leave it
to the application to post their pod's requests in that format.

## Design
The design of NHD is very similar to the Vanilla Kubernetes cluster (https://github.com/kubernetes/kubernetes/blob/release-1.1/docs/devel/scheduler.md). Custom Kubernetes schedulers are run as regular pods, but have special permissions for binding objects (pods) to nodes. From a high level, the scheduler sits in an infinite loop waiting for events from the Kubernetes API server to tell it a pod needs to be scheduled, and takes any action, if necessary. The ```schedulerName``` field in the Usage section above is what prevents race conditions from the default scheduler to any third-party scheduler. It is the responsibility of the scheduler to only modify pods that are assigned to that scheduler, otherwise it creates a race condition. 

Once NHD sees a pod assigned to it, the following actions are taken in each scheduler loop:

* Filter out the nodes with a set of predicates
* Assign a priority to each node based on the nodes that passed the filter
* Choose a node to schedule on

NHD does not use the watch API for pod events. While that API is much more efficient than the alternatives, it's not guaranteed that messages are received in order, and if NHD is restarted, events may be completely missed. Therefor, it must do its reconciliation loop completely from information other than events. Events may be used in the future to supplement other methods for caching, but they cannot be used as the primary control loop mechanism.

### Node Filtering
The node filtering step is very different from the default scheduler in that the reasons for filtering are based almost entirely on the topology features of the pod and nodes, and not on the current state of the node. For example, the default will typically check disk pressure, CPU usage, and memory usage before scheduling onto a node. In the NHD case, we assume there is no disk pressure since our pods don't write heavily to disk (this may change), and the CPU/memory measurements are not based on heuristics. Instead, NHD keeps track of hardware that has already been consumed, and _does not allow pods to share CPU cores or GPUs_. The only shareable resource in NHD is a network interface, and this is done based on a pod's pre-defined network consumption estimates.

At the moment, these are the attributes that NHD filters on:

* CPU cores (both SMT and non-SMT)
* GPUs
* NICs
* Memory (Hugepages)

#### CPU Cores
As mentioned above, CPU cores are an exclusive resource, and are not allowed to be shared across pods. A node may either have SMT enabled or disabled, so the cores are treated differently in each case. It is the pod's responsibility to specify whether certain functions are allowed to share a sibling core, or if they must have a completely isolated physical core.

#### GPUs
A pod consumes zero or more entire GPUs; since NHD doesn't allow GPU sharing, no other pod will utilize those devices. All GPUs are not created equal, and it's important which CPU and NIC the GPU maps to. For that reason, the k8s device plugin from Nvidia (https://github.com/NVIDIA/k8s-device-plugin) cannot be used since it masks the GPU device ID, and provides no NUMA or PCIe topology guarantees. NHD is responsible for tracking which physical device IDs are used, as well as whether the unused ones can service a pod based on its topology. A pod may also request a specific model of GPU to guarantee consistent performance.

#### NICs
NICs are the only shareable resource allowed by NHD. This is done because the interface speeds may be 100Gbps or more, and it's very likely a single pod does not need to consume the entire interface. Only the secondary interface(s) on the system are schedulable by NHD, since the primary interface is typically control/management plane and does not need topology guarantees.

To schedule NIC resources, the pod gives a hint as to how much bandwidth is needed per CPU core. NHD accumulates all bandwidth requests, and attempts to find one or more interfaces feasible for the request. If a request is feasible, the interface information is annotated in the pod spec.

### Mapping Type
Currently two mapping types are supported: NUMA and PCI. NUMA mapping attempts to map all CPU cores, GPUs, and NICs in the same processing group onto the same NUMA node. PCI mapping does the same thing, but it also ensures that any GPU and NIC in the same processing group are on the same PCIe switch.

# Debugging
To debug deployment issues with NHD, most issues can be seen by either looking at Kubernetes events, or the log of NHD. Only major events will be shown in the Kubernetes event log. All NHD events will start with the string "NHD", and can be filtered with grep. For example, to view events in my-namespace:

```
└──> $ kubectl get events -n my-namespace | grep NHD
6m19s       Normal   StartedScheduling   Pod           NHD: Started scheduling my-namespace/mypod-0
6m19s       Normal   Scheduling          Pod           NHD: Node fi-gcomp001.nae05.v3gdev.viasat.io selected for scheduling
6m19s       Normal   CfgMapSuccess       Pod           NHD: Successfully replaced ConfigMap contents. Binding pod to node
6m19s       Normal   Scheduled           Pod           NHD: Successfully assigned my-namespace/mypod-0 to fi-gcomp001.nae05.v3gdev.viasat.io
```

For more detailed debugging, the NHD logs provide information from the point the pod tries to be schedule, through the resource allocation process, all the way to binding to the node. The NHD log is typically very verbose, and in busy cluster should be used with the --tail option:

```
└──> $ kubectl logs nhd-5cf5c64676-kmtzz --tail=100
2019-11-19:18:09:46.593 INFO     [Matcher.py:286] Removing {(1,)} from GPU sets since they're not in the intersection
2019-11-19:18:09:46.593 INFO     [Matcher.py:292] Removing CPU combo (1, 0) from set since it's not in intersection
2019-11-19:18:09:46.593 INFO     [Matcher.py:292] Removing CPU combo (1, 1) from set since it's not in intersection
2019-11-19:18:09:46.593 INFO     [Matcher.py:308] Node fi-gcomp001.nae05.v3gdev.viasat.io has 1 valid resource combos after intersection. Leaving as candidate
2019-11-19:18:09:46.593 INFO     [Matcher.py:269] Set intersection is [(0,)]
...
```

# Roadmap
Since NHD was developed internally at Viasat, there are some things that need to be made common to make it easier for others to use. In addition to that, there are some performance enhancements that can be made to the scheduler and to the deployments to improve speed and application performance. These items are roughly listed in order of priority:

## More Documentation!
More documentation is needed for debugging issues, deploying pods, and determing whether the resource utilization is correct.

## Common config format
A common configuration format published by the pods as a ConfigMap will leave the application-specific knowledge out of NHD.

## SR-IOV Support With Device Plugin
The SR-IOV device plugin (https://github.com/intel/sriov-network-device-plugin) provides a way to allocate VFs as resources to pods. 

