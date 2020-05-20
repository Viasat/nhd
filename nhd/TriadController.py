import asyncio
import contextlib
import threading
import time
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from nhd.NHDWatchQueue import qinst
from nhd.NHDWatchQueue import NHDWatchTypes
from nhd.NHDCommon import NHDCommon
from nhd.NHDScheduler import NHD_SCHED_NAME
import kopf
import yaml

# The operator class starts up new pods matching a certain type of CRD. The NHD schedule is agnostic to CRDs, 
# so the pods must either be started from a statefulset or some other controller.


@kopf.on.create('sigproc.viasat.io', 'v1', 'triadsets')
def TriadSetCreate(spec, meta, **_):
    logger = NHDCommon.GetLogger(__name__)
    try:
        config.load_incluster_config()
    except:
        config.load_kube_config()

    v1   = client.CoreV1Api()   

    logger.info(f'Found new TriadSet for component {spec["serviceName"]} with {spec["replicas"]} replicas in namespace {meta["namespace"]}')
    logger.info('Creating pods...')

    podspec = yaml.dump(spec["template"])
    
    # Indent the pod spec to line up with the rest of the yaml
    podspec = f"apiVersion: v1\nkind: Pod\n{podspec}"

    # Reload the yaml to patch some fields
    podyaml = yaml.safe_load(podspec)

    # Start up all the pods in our set
    for ord in range(int(spec["replicas"])):
        logger.info(f'Creating pod {meta["name"]} pod with ordinal index {ord}')
        podyaml['metadata']['name'] = f'{meta["name"]}-{ord}' # Give it the canonical statefulset-type name
        kopf.adopt(podyaml)

        v1.create_namespaced_pod(namespace = meta['namespace'], body = podyaml)


@kopf.on.delete('sigproc.viasat.io', 'v1', 'triadsets')
def delete_fn(**_):
    logger = NHDCommon.GetLogger(__name__)
    logger.info('Received delete request for TriadSet')

    # We don't really need to do anything here since the TriadSet owns all the pods beneath it, and Kubernetes will clean that


@kopf.on.update('', 'v1', 'nodes')
#@kopf.on.field('', 'v1', 'nodes', field='spec.unschedulable')
#def TriadNodeUpdate( meta, old, new, **_):
def TriadNodeUpdate(spec, old, new, meta, **_):
    logger = NHDCommon.GetLogger(__name__)
    NHDTainted = lambda obj: any([x['key'] == 'sigproc.viasat.io/nhd_scheduler' for x in obj['spec']['taints']])

    k8sq = qinst
    # If the NHD taint has been added/removed or the code has been cordoned/uncordoned, detect it here
    if (not NHDTainted(old) and NHDTainted(new)) or ('unschedulable' in old['spec'] and 'unschedulable' not in new['spec']): # Uncordon
        logger.info(f'Uncordoning node {meta["name"]}')
        k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_NODE_UNCORDON, "node": meta["name"]})
    elif (not NHDTainted(new) and NHDTainted(old)) or ('unschedulable' not in old['spec'] and 'unschedulable' in new['spec']): # Cordon:
        logger.info(f'Cordoning node {meta["name"]}')
        k8sq.put({"type": NHDWatchTypes.NHD_WATCH_TYPE_NODE_CORDON, "node": meta["name"]})

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


# DO NOT DEFINE on.update! It's handled elsewhere

def TriadControllerLoop(
        ready_flag: threading.Event,
        stop_flag: threading.Event,
):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    with contextlib.closing(loop):

        kopf.configure(verbose=False)  # log formatting

        loop.run_until_complete(kopf.operator(
            ready_flag=ready_flag,
            stop_flag=stop_flag,
        ))
