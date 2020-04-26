import logging
import os
import datetime
import random
import string
from nhd.NHDCommon import NHDCommon
from enum import Enum
from colorlog import ColoredFormatter
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from nhd.Node import Node
from typing import Dict, List, Set, Tuple
import magicattr


class K8SEventType(Enum):
    EVENT_TYPE_NORMAL = 0
    EVENT_TYPE_WARNING = 1

"""
Helper class to communicate with Kubernetes API server. Assumes we are either running in a pod with
proper permissions, or we have a KUBECONFIG file with cluster information.
"""
class K8SMgr:
    __instance = None
    
    @staticmethod
    def GetInstance():
        if K8SMgr.__instance == None:
            K8SMgr()

        return K8SMgr.__instance

    def __init__(self):
        """
        Initializes the logger and loads Kubernetes configuration
        """
        self.logger = NHDCommon.GetLogger(__name__)

        #config.load_incluster_config()
        if K8SMgr.__instance != None:
            raise Exception("Cannot create more than one K8SMgr!")
        else:
            try:
                config.load_incluster_config()
            except:
                config.load_kube_config()

            self.v1 = client.CoreV1Api()
            self.last_seen_ver = None

            K8SMgr.__instance = self

    def GetNodes(self):
        """ Get the list of all currently-ready nodes """
        nodes = []
        try: 
            a = self.v1.list_node(watch=False)
            for i in a.items:
                for status in i.status.conditions:
                    if status.status == "True" and status.type == "Ready":
                        nodes.append(i.metadata.name)
        except ApiException as e:
            self.logger.error("Exception when calling CoreV1Api->list_node: %s\n" % e)

        return nodes
                
    def GetNodeHugepageResources(self, node: str):
        """
        Pulls the hugepage resource information from a node (requests/allocatable)
        """
        try: 
            a = self.v1.read_node(name = node)
            alloc = int(a.status.allocatable['hugepages-1Gi'][:a.status.allocatable['hugepages-1Gi'].find('G')])

            # The actual amount of allocatable hugepages must be retrieved by iterating over each pod on this node
            free = alloc
            pods = self.v1.list_pod_for_all_namespaces(watch=False)
            for i in pods.items:
                if i.status.phase in ('Running', 'ContainerCreating', 'Pending'):
                    if i.spec.node_name != node:
                        continue

                    for c in i.spec.containers:
                        if c.resources.requests and 'hugepages-1Gi' in c.resources.requests:
                            free -= int(c.resources.requests['hugepages-1Gi'][:c.resources.requests['hugepages-1Gi'].find('G')])


            return (alloc, free)

        except ApiException as e:
            self.logger.error("Exception when calling CoreV1Api->list_node: %s\n" % e)
        except Exception as e:
            self.logger.error("Non-API exception when getting hugepage information")
        
        return (0,0)

    def GetNodeAttr(self, name, attr):
        """
        Get an attribute from a node. Useful for pulling things like nested data structures.
        """
        val = None
        try: 
            a = self.v1.read_node(name = name)
            for status in a.status.conditions:
                if status.status == "True" and status.type == "Ready":
                    return magicattr.get(a, attr)

        except ApiException as e:
            self.logger.error("Exception when calling CoreV1Api->list_node: %s\n" % e)

    def GetNodeAddr(self, name):
        return self.GetNodeAttr(name, 'status.addresses[0].address')

    def GetNodeLabels(self, name):
        return self.GetNodeAttr(name, 'metadata.labels')

    def GetPodNode(self, pod, ns):
        """
        Get the node a pod resides on
        """
        ret = self.v1.read_namespaced_pod(pod, ns)
        if ret == None:
            return None
        
        return ret.spec.node_name

    def GetPodObj(self, pod, ns):
        try: 
            pobj = self.v1.read_namespaced_pod(pod, ns)
            return pobj
        except ApiException as e:
            self.logger.error("Exception when calling CoreV1Api->read_namespaced_pod: %s\n" % e)  

    def GetCfgAnnotations(self, pod, ns):
        """ Get the config annotations from the pod """
        try: 
            annot = self.GetPodAnnotations(ns, pod)
            if annot == None or 'sigproc.viasat.io/nhd_config' not in annot:
                self.logger.error(f'Couldn\'t find pod annotations for pod {ns}.{pod}')
                return False

            return annot['sigproc.viasat.io/nhd_config']
        except ApiException as e:
            self.logger.error("Exception when calling CoreV1Api->read_namespaced_pod: %s\n" % e)          
        
        return False

    def GetPodNodeGroup(self, pod, ns) -> str:
        """ Returns the node group name of the pod, or "default" if it doesn't exist. """
        try:
            p = self.v1.read_namespaced_pod(pod, ns)
        except:
            self.logger.warning(f"Failed to get pod annotations for pod {pod} in namespace {ns}")
            return "default"

        if 'sigproc.viasat.io/nhd_group' in p.metadata.annotations:
            group = p.metadata.annotations["sigproc.viasat.io/nhd_group"]
            self.logger.info(f'Pod is using NHD group {group}') 
            return str(group)

    def IsNHDTainted(self, node):
        """
        Find out if the node is tainted for NHD. Only tainted nodes will be used by NHD for scheduling, and also
        ignored by the default scheduler.
        """
        candidate = False
        try: 
            a = self.v1.read_node(name = node)        
            taints = a.spec.taints
            
            for t in taints:
                if t.key == 'sigproc.viasat.io/nhd_scheduler' and t.effect == 'NoSchedule':
                    candidate = True
                if t.key == 'node.kubernetes.io/unschedulable':
                    self.logger.warning(f'Node {node} disabled scheduling. Removing from list')
                    candidate = False
                    break                   

        except Exception:
            return False

        return candidate

    def GetPodAnnotations(self, ns, podname):
        try:
            p = self.v1.read_namespaced_pod(podname, ns)
            return p.metadata.annotations
        except ApiException as e:
            self.logger.error("Exception when calling CoreV1Api->read_namespaced_pod: %s\n" % e)    
            return None 

        return None       


    def GetScheduledPods(self, sched_name):
        """
        Get all scheduled pods for a given scheduler
        """        
        ret = self.v1.list_pod_for_all_namespaces()
        pods = []

        for i in ret.items:
            if i.spec.scheduler_name == sched_name:
                pods.append((i.metadata.name, i.metadata.namespace, i.status.phase))

        return pods

    def GetRequestedPodResources(self, pod: str, ns: str) -> Dict[str, str]:
        """
        Get the pod resources in dict format
        """        
        try: 
            p = self.v1.read_namespaced_pod(pod, ns)

            # Only support one container per pod for now
            return p.spec.containers[0].resources.requests
        except ApiException as e:
            self.logger.error("Exception when calling CoreV1Api->read_namespaced_pod: %s\n" % e)

        return {}



    def ServicePods(self, sched_name):
        """ Check if a pod is waiting to be scheduled """
        ret = self.v1.list_pod_for_all_namespaces()
        pods = {}
        for i in ret.items:
            if i.spec.scheduler_name != sched_name:
                continue

            pods[(i.metadata.namespace, i.metadata.name, i.metadata.uid)] = (i.status.phase, i.spec.node_name)

#            if event['object'].status.phase == "Pending" and event['object'].spec.node_name is None:
#                try:
#                    pods.append((event['object'].metadata.name, event['object'].metadata.namespace, PodStatus.POD_STATUS_PENDING))
#                except client.rest.ApiException as e:
#                    self.logger.error(json.loads(e.body)['message'])                
#
#            elif event['object'].status.phase == "Failed":
#                pods.append((event['object'].metadata.name, event['object'].metadata.namespace, PodStatus.POD_STATUS_FAILED))

        return pods

    def FlushWatchQueue(self):
        self.logger.info('Flushing watch queue')
        w = watch.Watch()
        if self.last_seen_ver == None:
            e = w.stream(self.v1.list_pod_for_all_namespaces)
        else:
            e = w.stream(self.v1.list_pod_for_all_namespaces, resource_version=self.last_seen_ver)
        
        for event in e:
            self.last_seen_ver = event['object'].metadata.resource_version


#    def ServicePods(self, sched_name):
        """ Switched to using a list of pods instead of watching """

#        w = watch.Watch()
#        if self.last_seen_ver == None:
#            e = w.stream(self.v1.list_pod_for_all_namespaces)
#        else:
#            e = w.stream(self.v1.list_pod_for_all_namespaces, resource_version=self.last_seen_ver)
#
#        for event in e:
#            self.last_seen_ver = event['object'].metadata.resource_version
#            if event['object'].spec.scheduler_name != sched_name:
#                continue
#
#            return (event['object'].metadata.name, 
#                    event['object'].metadata.namespace, 
#                    event['object'].status.phase, 
#                    event['object'].spec.node_name,
#                    event['type'])
#        
#        return None
#                print(event['object'].status.phase, event['object'].metadata.name)
#                if event['object'].status.phase == "Pending" and  event['object'].spec.node_name is None:
#                    try:
#                        self.logger.info(f"Received pod scheduling request for {event['object'].metadata.name}")
#                        return event['object'].metadata.name
#                    except client.rest.ApiException as e:
#                        self.logger.error(json.loads(e.body)['message'])

    def AddNADToPod(self, pod, ns, nads):
        """ Adds network attachment definitions to bind to pod """
        try:
            self.v1.patch_namespaced_pod(pod, ns, body= {
                "metadata": {
                    "annotations": {
                        "k8s.v1.cni.cncf.io/networks": nads
                    }
                }
            })
        except ApiException as e:
            self.logger.error(f'Failed to update pod metadata NAD {pod} in namespace {ns}')
            return False

        return True

    def AddSRIOVDevice(self, pod, ns, device, num):
        """ Adds an SR-IOV device using the SR-IOV plugin. Only adds to the first container. Unfortunately Kubernetes
            does not allow you to patch resources of a pod, so we must replace the container. """
        self.logger.info(f'Adding {num} SR-IOV device{"s" if num > 0 else ""} {device} to pod {ns}.{pod}')

        # This does NOT work. Even replacing a pod to update resources does not work. There's an outstanding KEP
        # To fix this, but it's still not available yet: 
        # https://github.com/kubernetes/community/pull/2908/commits/4ad6fa7c27f4a21c27a6be83c2dc81c43549fa55
        try:
            p = self.v1.read_namespaced_pod(pod, ns)
        except ApiException as e:
            self.logger.error(f'Failed to get pod spec {pod} in namespace {ns}')
            return False
        
        # Only add to first container
        p.spec.containers[0].resources.limits[f'intel.com/{device}']   = f'{num}'
        p.spec.containers[0].resources.requests[f'intel.com/{device}'] = f'{num}'
        
        try:
            self.v1.replace_namespaced_pod(pod, ns, body=p)
        except ApiException as e:
            self.logger.error(f'Failed to replace pod spec {pod} in namespace {ns}')
            print(e)
            return False

        self.logger.info(f'Added SR-IOV device into pod {pod}')
        return True


    def GetCfgMap(self, pod, ns):
        """
        Gets the first configmap from an existing pod
        """
        ret = self.v1.list_namespaced_pod(watch=False, namespace=ns)
        for i in ret.items:
            if i.metadata.name != pod: # Only look at container that use the run command
                continue
            
            cm = None
            for v in i.spec.volumes:
                if v.config_map:
                   cm = v.config_map.name
                   break

            if cm:
                self.logger.info(f'Found base ConfigMap {cm} for pod {pod}')
                cmdat = self.v1.list_namespaced_config_map(ns)
                for c in cmdat.items:
                    if c.metadata.name != cm:
                        continue

                    self.logger.info(f'Successfully looked up ConfigMap {cm}')
                    for cname, cval in c.data.items():
                        self.logger.info(f'Returning ConfigMap for file {cname}')
                        return (cm, cval)
            else:
                self.logger.error(f'No ConfigMap found for pod {pod}')
                return (None,None)

        return (None,None)

    def AnnotatePodConfig(self, ns, podname, configstr):
        """ Annotate the pod's configuration """
        try:
            self.v1.patch_namespaced_pod(podname, ns, body= {
                "metadata": {
                    "annotations": {
                        "sigproc.viasat.io/nhd_config": configstr
                    }
                }
            })
        except ApiException as e:
            self.logger.error(f'Failed to update pod metadata configuration for {podname} in namespace {ns}')
            return False        

        return True

    def PatchConfigMap(self, ns, cmname, cmbody):
        """ Patches a ConfigMap object in place with a new value """

        try:
            resp = self.v1.read_namespaced_config_map(name=cmname, namespace=ns)
            keyname = list(resp.data.keys())[0]
            tmp_map = {
                "kind": "ConfigMap",
                "apiVersion": "v1",
                "metadata": {
                    "name": cmname,
                },
                "data": {
                    keyname: cmbody
                }
            }

            ret = self.v1.patch_namespaced_config_map(name=cmname, namespace=ns, body=tmp_map)

        except ApiException as e:
            self.logger.error(f'Failed to replace configmap {cmname} in namespace {ns}')
            return False

        return True

    # def GetKeyFromConfigMap(self, ns, cmname):
    #     """ Returns the name of the first key in a configmap """
    #     try:
    #         resp = self.v1.read_namespaced_config_map(name=cmname, namespace=ns)     
    #         keyname = list(resp.data.keys())[0]
    #         return keyname   
    #     except ApiException as e:
    #         self.logger.error(f'Failed to get keyname from configmap {cmname} in namespace {ns}')

    #     return ''        

    # def CopyConfigMap(self, ns, oldcm, cmbody):
    #     """ Replaces a ConfigMap object with a new one """
    #     cmname = "nhd-config" + self.GetRandomUid()

    #     try:
    #         keyname = self.GetKeyFromConfigMap(ns, oldcm)
    #         tmp_map = {
    #             "kind": "ConfigMap",
    #             "apiVersion": "v1",
    #             "metadata": {
    #                 "name": cmname,
    #             },
    #             "data": {
    #                 keyname: cmbody
    #             }
    #         }

    #         ret = self.v1.create_namespaced_config_map(body=tmp_map, namespace=ns)

    #     except ApiException as e:
    #         self.logger.error(f'Failed to create configmap {cmname} in namespace {ns}')
    #         return False

    #     return cmname
     
    # def ReplaceVolumeMountConfigMap(self, podname, ns, oldcm, newcm):
    #     """ Replaces the configmap object in the volume mount of an old configmap """    
    #     try:
    #         ret = self.v1.read_namespaced_pod(podname, ns)
    #     except ApiException as e:
    #         self.logger.error(f'API exception when fetching namespaced pod: {ns}.{podname}: {e}')
    #         return False

    #     for v in ret.spec.volumes:
    #         if v.config_map is not None:
    #             if v.config_map.name == oldcm:
    #                 # We want to replace this configmap object
                    

                                  

    def BindPodToNode(self, podname, node, ns):
        """ Binds a pod to a node to start the deployment process. """
        try:
            target        = client.V1ObjectReference()
            target.kind   = "Node"
            target.apiVersion = "v1"
            target.name   = node
            
            meta          = client.V1ObjectMeta()
            meta.name     = podname
            body          = client.V1Binding(target=target, metadata=meta)
            body.target   = target
            body.metadata = meta

            return self.v1.create_namespaced_binding(namespace=ns, body=body)

        except ApiException as e:
            self.logger.error(f'Failed to bind pod {podname} to node {node} in namespace {ns}')
            return False
        except ValueError as e:
            # This is not a real error. It's a problem in the API waiting to be fixed:
            # https://github.com/kubernetes-client/python/issues/547
            pass

        return True

    def GetCfgType(self, pod: str, ns: str) -> str:
        """
        Gets the configuration type from the pod's annotations
        """
        try:
            ret = self.v1.read_namespaced_pod(pod, ns)
            # Verify all annotations are present
            ann = ret.metadata.annotations     
            return ann['sigproc.viasat.io/cfg_type']   
        except KeyError as e:
            self.logger.error('Key error when fetching namespaced pod')
            return ''
        except ApiException as e:
            self.logger.error(f'API exception when fetching namespaced pod: {ns}.{pod}: {e}')
            return ''

    def GetTimeNow(self) -> str:
        """ Uses Kubernetes undocumented format. Any deviation from this will throw an error at the API server """
        return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    def GetRandomUid(self) -> str:
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(15))

    def GeneratePodEvent(self, podobj, podname, ns, reason, _type, message):
        """ Generates a pod event on the kubernetes API server """
        try:
            meta  = client.V1ObjectMeta()
            meta.name = f'{podname}.{self.GetRandomUid()}'
            meta.namespace = ns

            invobj = client.V1ObjectReference()
            invobj.name = podname
            invobj.kind = "Pod"
            invobj.namespace = ns
            invobj.api_version = 'v1'
            invobj.uid = podobj.metadata.uid

            evtsrc = client.V1EventSource()
            evtsrc.component = 'NHD Scheduler'

            if _type == K8SEventType.EVENT_TYPE_NORMAL:
                etype = "Normal"
                lg = self.logger.info
            else:
                etype = "Warning"
                lg = self.logger.warning

            timestamp = self.GetTimeNow()


            # Log an event in our pod too instead of duplicating externally
            lg(f'Event for pod {ns}/{podname} -- Reason={reason}, message={message}')
            event = client.V1Event( involved_object=invobj, 
                                    source = evtsrc, 
                                    metadata=meta, 
                                    reason=reason, 
                                    message=f'NHD: {message}', 
                                    count=1, 
                                    type=etype, 
                                    first_timestamp=timestamp, 
                                    last_timestamp=timestamp)

            self.v1.create_namespaced_event(namespace=ns, body=event)

        except ApiException as e:
            self.logger.error(f'Failed to send event for pod {podname}: {e}')


