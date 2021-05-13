import asyncio
import contextlib
import threading
import time
import logging
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from nhd.NHDWatchQueue import qinst
from nhd.NHDWatchQueue import NHDWatchTypes
from nhd.NHDCommon import NHDCommon
from nhd.NHDCommon import NHDLock
from nhd.NHDScheduler import NHD_SCHED_NAME
from nhd.Node import Node
import kopf
import yaml
import os


@kopf.on.startup()
def Configure(settings: kopf.OperatorSettings, **kwargs):
    # kopf logs into our handler and we see duplicates if we don't shut it off
    logging.getLogger().handlers[:] = []
    settings.persistence.finalizer = 'sigproc.viasat.io/nhd-finalizer'
 

# TriadSet created -- nothing to do here since our timer will create the pods
@kopf.on.create('sigproc.viasat.io', 'v1', 'triadsets')
def TriadSetCreate(spec, meta, **_):
    logger = NHDCommon.GetLogger(__name__)
    logger.info(f'Found new TriadSet for component {spec["serviceName"]} with {spec["replicas"]} replicas in namespace {meta["namespace"]}')


# TriadSet deleted. Nothing really to do here since k8s will tear the pods down as part of the ownership
@kopf.on.delete('sigproc.viasat.io', 'v1', 'triadsets')
def delete_fn(meta, **_):
    logger = NHDCommon.GetLogger(__name__)
    logger.info('Received delete request for TriadSet')


# Detect node changes to determine if a node is cordoned and/or the NHD group label is changing
@kopf.on.update('', 'v1', 'nodes')
def TriadNodeUpdate(spec, old, new, meta, **_):
    logger = NHDCommon.GetLogger(__name__)
    NHDTainted = lambda obj: any([x['key'] == 'sigproc.viasat.io/nhd_scheduler' for x in obj['spec']['taints']])

    k8sq = qinst
    # If the NHD taint has been added/removed or the code has been cordoned/uncordoned, detect it here
    if (not NHDTainted(old) and NHDTainted(new)) or (('unschedulable' in old['spec'] and 'unschedulable' not in new['spec']) and NHDTainted(new)): # Uncordon
        logger.info(f'Uncordoning node {meta["name"]}')
        k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_NODE_UNCORDON, "node": meta["name"]})
    elif (not NHDTainted(new) and NHDTainted(old)) or ('unschedulable' not in old['spec'] and 'unschedulable' in new['spec']): # Cordon:
        logger.info(f'Cordoning node {meta["name"]}')
        k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_NODE_CORDON, "node": meta["name"]})

    # Detect NHD group changes. If the label didn't exist, or it's now different than the old one, send the new one
    if ('NHD_GROUP' not in old['metadata']['labels'] and 'NHD_GROUP' in new['metadata']['labels']) or \
       ('NHD_GROUP' in old['metadata']['labels'] and 'NHD_GROUP' in new['metadata']['labels'] and old['metadata']['labels'] != new['metadata']['labels']):

       logger.info(f'Updating NHD group for node {meta["name"]} to {new["metadata"]["labels"]["NHD_GROUP"]}')
       k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_GROUP_UPDATE, "node": meta["name"], "groups": new['metadata']['labels']['NHD_GROUP']})
    elif ('NHD_GROUP' in old['metadata']['labels']) and ('NHD_GROUP' not in new['metadata']['labels']): # Label removed

       logger.info(f'Updating NHD group for node {meta["name"]} to default')
       k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_GROUP_UPDATE, "node": meta["name"], "groups" : "default"})

    # Detect change in node maintenance state
    oldMaintenance = Node.GetMaintenance(old['metadata']['labels'])
    newMaintenance = Node.GetMaintenance(new['metadata']['labels'])
    if (not oldMaintenance and newMaintenance):
        logger.info(f'Starting Maintenance for node {meta["name"]}')
        k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_NODE_MAINT_START, "node": meta["name"]})
    elif (oldMaintenance and not newMaintenance):
        logger.info(f'Ending Maintenance for node {meta["name"]}')
        k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_NODE_MAINT_END, "node": meta["name"]})


# Timer acting as the TriadSet controller. Pods under the set are only created here either by a new set appearing, or an
# existing pod being deleted.
@kopf.timer('sigproc.viasat.io', 'v1', 'triadsets', interval = 3.0, idle = 3.0)
async def MonitorTriadSets(spec, meta, **kwargs):
    logger = NHDCommon.GetLogger(__name__)

    nhd_lock = NHDLock.GetInstance()
    with nhd_lock.GetLock():
        logger.debug(f'Kicking off controller timer for {meta["namespace"]}/{meta["name"]}')
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.CoreV1Api()

        for ord in range(spec['replicas']):
            podname = f'{spec["serviceName"]}-{ord}'
            try:
                _ = v1.read_namespaced_pod(name = podname, namespace = meta["namespace"])
            except ApiException as e:
                logger.info(f'Triad pod {podname} not found in namespace {meta["namespace"]}, but TriadSet is still active. Restarting pod')
                podspec = yaml.dump(spec["template"])
            
                # Indent the pod spec to line up with the rest of the yaml
                podspec = f"apiVersion: v1\nkind: Pod\n{podspec}"

                # Reload the yaml to patch some fields
                podyaml = yaml.safe_load(podspec)
                podyaml['metadata']['name'] = podname # Give it the canonical statefulset-type name

                # Patch in the hostname and subdomain to create a DNS record like a statefulset
                podyaml['spec']['hostname']  = podname
                podyaml['spec']['subdomain'] = meta["name"]
                kopf.adopt(podyaml)
                obj = v1.create_namespaced_pod(namespace = meta['namespace'], body = podyaml)


# Triad pod being created
@kopf.on.create('', 'v1', 'pods',
                annotations={'sigproc.viasat.io/cfg_type': 'triad'},
                when=lambda spec, **_: spec.get('schedulerName') == NHD_SCHED_NAME)
def TriadPodCreate(spec, meta, **_):
    logger = NHDCommon.GetLogger(__name__)
    logger.info(f'Saw new Triad pod {meta["namespace"]}.{meta["name"]}')

    k8sq = qinst # Get the watch queue so we can notify NHD of events from the controller
    k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_TRIAD_POD_CREATE, "pod": {"ns": meta["namespace"], "name": meta["name"]}})

# Triad pod being deleted
@kopf.on.delete('', 'v1', 'pods',
                annotations={'sigproc.viasat.io/cfg_type': 'triad'},
                when=lambda spec, **_: spec.get('schedulerName') == NHD_SCHED_NAME)
def TriadPodDelete(spec, meta, **_):
    logger = NHDCommon.GetLogger(__name__)   
    logger.info(f'Saw deleted Triad pod {meta["namespace"]}.{meta["name"]}')

    k8sq = qinst # Get the watch queue so we can notify NHD of events from the controller
    k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_TRIAD_POD_DELETE, "pod": {"ns": meta["namespace"], "name": meta["name"]}})        

def HandleExceptions(loop, context):
    logger = NHDCommon.GetLogger(__name__)   
    msg = context.get("exception", context["message"])
    logger.error(f"Caught exception: {msg}")
    logger.info("Shutting down...")
    os._exit(-1) # Kill entire application and let k8s restart it. No state needs to be preserved


# DO NOT DEFINE on.update! It's handled elsewhere

def TriadControllerLoop(
        ready_flag: threading.Event,
        stop_flag: threading.Event,
):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    with contextlib.closing(loop):

        kopf.configure(verbose=False)  # log formatting

        loop.set_exception_handler(HandleExceptions)
        loop.run_until_complete(kopf.operator(
            ready_flag=ready_flag,
            stop_flag=stop_flag,
        ))
