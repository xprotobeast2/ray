from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import yaml
import random
import time

import kubernetes.client as k8sclient
import kubernetes.config as k8sconfig

from kubernetes.client.rest import ApiException

from ray.autoscaler.node_provider import NodeProvider, DEFAULT_CONFIGS
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME, TAG_RAY_LAUNCH_CONFIG, TAG_RAY_NODE_TYPE
from ray.autoscaler.autoscaler import ConcurrentCounter

MAX_POLLS = 100
POLL_INTERVAL = 5
REDIS_ACCESS_SERVICE_PORT = 6379


class KubernetesNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

        self.head_pod_spec = None
        self.redis_address = None
        self.redis_service_created = False
        # Set namespace this cluster is running in
        try:    
            self.namespace = provider_config["namespace"]
        except KeyError as e:
            print("Must provide namespace in config file\
                when using KubernetesNodeProvider.\n %s" % e)
            return
        
        # Check from where we are launching cluster     
        if "KUBERNETES_SERVICE_HOST" in os.environ:
            # We're already in the k8s pod here
            # Load in cluster config
            k8sconfig.load_incluster_config()

            # Initialize client api objects
            self.client_v1 = k8sclient.CoreV1Api()

            # Introspect pod image
            try:
                self.head_pod_spec = self.client_v1.read_namespaced_pod(
                    name=os.environ['HOSTNAME'],
                    namespace=self.namespace,
                    export=True)
            except:
                print("Error retrieving head pod config: %s\n", e)

        else:
            # We're off cluster, we need to start a k8s cluster
            try:
                # Load the kube config
                k8sconfig.load_kube_config()

                # Initialize client api objects
                self.client_v1 = k8sclient.CoreV1Api()
                
            except Exception as e:
                # No k8s service or incorrectly configured
                print("Exception trying load kube config: %s\n" % e)
                return

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_pods = {}
        self.idx = 0

    def nodes(self, tag_filters):
        """Return a list of pod labels filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to nodes() to
        serve single-node queries (e.g. is_running(node_id)). This means that
        nodes() must be called again to refresh results.

        Examples:
            >>> provider.nodes({TAG_RAY_NODE_TYPE: "worker"})
            ["node-1", "node-2"]
        """
        # Select labels by tag filter
        label_selector = ",".join(
            ["{tag}={val}".format(tag=k, val=v) 
            for (k,v) in tag_filters.items()
        ])
        # Make call to the k8s master having applied the filters
        try:
            # Get pods
            pod_list = self.client_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector)

        except ApiException as e:
            print("Exception when listing pods: %s\n" % e)

        # Now apply the status filters
        pods = [pod for pod in pod_list.items if pod.status.phase in ["Running", "Pending"]]
        # Cache pods and available
        self.cached_pods = {pod.metadata.name:pod for pod in pods}

        return [pod.metadata.name for pod in pods]
    
    def is_running(self, node_id):
        """Return whether the specified node is running."""
        node = self._node(node_id)
        return node.status.phase=="Running"

    def is_terminated(self, node_id):
        """Return whether the specified node is terminated."""
        node = self._node(node_id)
        return node.status.container_statuses[0] not in ["running", "waiting"]

    def node_tags(self, node_id):
        """Returns the tags of the given node (string dict)."""
        node = self._node(node_id)
        return node.metadata.labels

    def external_ip(self, node_id):
        """Returns the external ip of the given node."""
        node = self._node(node_id)
        return node.status.pod_ip

    def internal_ip(self, node_id):
        """Returns the internal ip (Ray ip) of the given node."""
        node = self._node(node_id)
        return node.status.pod_ip
        
    def create_node(self, node_config, tags, count):
        """Creates a number of nodes within the namespace."""

        pod_body = None
        svc_body = None

        # What should the node look like
        if tags[TAG_RAY_NODE_TYPE] == 'head':
            # If we're trying to create a head node, this is off cluster, use node config
            pod_body = node_config["pod"]
            svc_body = node_config["services"]

        elif self.head_pod_spec is not None:
            # In this case we're trying to create a worker and we're in k8s cluster
            pod_body = self.head_pod_spec.to_dict()
            # get the image then swap out the containers
            image = pod_body["spec"]["containers"][0]["image"]
            pod_body["spec"]["containers"][0].update(node_config["pod"]["spec"]["containers"][0])
            pod_body["spec"]["containers"][0]["image"] = image
            # Secrets can't be mounted this way?
            del pod_body["spec"]["volumes"]

            if not self.redis_service_created:
                # Delete existing redis-services
                try:
                    self.client_v1.delete_namespaced_service(
                        name='ray-redis-service',
                        namespace=self.namespace,
                        body = k8sclient.V1DeleteOptions())
                    time.sleep(POLL_INTERVAL)
                except ApiException as e:
                    print("Error terminating service: %s\n " % e)

                # Set up redis service
                self.redis_address = node_config["redis_address"]
                redis_port = self.redis_address.split(':')[1]
                
                # Define a service port for workers to discover the redis server
                svc_port = k8sclient.V1ServicePort(
                    name='redis',
                    port=REDIS_ACCESS_SERVICE_PORT,
                    target_port=int(redis_port))
                
                svc_body = k8sclient.V1Service(
                    metadata=k8sclient.V1ObjectMeta(name='ray-redis-service') ,
                    spec=k8sclient.V1ServiceSpec(
                        selector=pod_body["metadata"]["labels"],
                        ports=[svc_port]
                        )
                    )
        # If a service needs to be created do it here
        if svc_body:
            try:
                self.client_v1.create_namespaced_service(
                    namespace=self.namespace,
                    body=svc_body)
                self.redis_service_created = True
            except ApiException as e:
                print("Error trying to create service: %s\n" % e)

        # Create the pod
        for _ in range(count):
            pod_name = tags[TAG_RAY_NODE_NAME] + '-' + str(self.idx)
            
            pod_body["metadata"]["name"] = pod_name
            pod_body["metadata"]["labels"] = tags
            
            try:
                self.client_v1.create_namespaced_pod(
                    namespace=self.namespace,
                    body=pod_body
                    )
                self.idx+=1
            except ApiException as e:
                print("Error creating pod %s: %s\n" %(pod_name, e))
            
            self._wait_for_pod_startup(pod_name)

    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        node = self._node(node_id)
        # Create the k8s patch body
        patch = {
        "metadata" : {
            "name": node_id,
            "labels": tags
            }
        }
        # Send the patch request
        try:
            self.client_v1.patch_namespaced_pod(
                name=node_id,
                namespace=self.namespace,
                body=patch)
        except ApiException as e:
            print("Exception when calling CoreV1Api->patch_namespaced_pod: %s\n" % e)

    def terminate_node(self, node_id):
        """Terminates the specified node."""
        try:
            self.client_v1.delete_namespaced_pod(
                name=node_id,
                namespace=self.namespace,
                body = k8sclient.V1DeleteOptions()
                )

        except ApiException as e:
            print("Error terminating pod %s : %s\n " % (node_id, e))

    def _node(self, node_id):
        """Check if pod info is cached otherwise request for pod by name"""
        if node_id in self.cached_pods:
            return self.cached_pods[node_id]
        # Make a REST call for the node by name
        try:
            node = self.client_v1.read_namespaced_pod(
                name=node_id,
                namespace=self.namespace)
        except ApiException as e:
            print("Exception when calling CoreV1Api->read_namespaced_pod: %s\n" % e)

        return node
    
    def _wait_for_pod_startup(self, node_id):
        """ Polls deployment status until status is updated """
        # Poll the pod status
        target_pod = self.client_v1.read_namespaced_pod(
            name=node_id,
            namespace=self.namespace
            )
        for _ in range(MAX_POLLS):
            try:
                # Wait for container to come up
                while target_pod.status.container_statuses is None:
                    time.sleep(1)
                    target_pod = self.client_v1.read_namespaced_pod(
                        name=node_id,
                        namespace=self.namespace)

                # Wait for iamge pull
                while target_pod.status.container_statuses[0].state == 'Waiting':
                    if target_pod.status.container_statuses[0].state.reason != 'ContainerCreating':
                        # Poll the pod status
                        target_pod = self.client_v1.read_namespaced_pod(
                            name=node_id,
                            namespace=self.namespace
                            )
                        time.sleep(POLL_INTERVAL)


                # Check the pod status
                if target_pod.status.phase in ["Running"] \
                and target_pod.status.container_statuses[0].ready:
                    print("Pod %s has container ready" % node_id)
                    return

                # Wait for a couple seconds
                time.sleep(POLL_INTERVAL)

            except Exception as e:
                print("Waiting for pod %s to come up %s\n" % (node_id,e))
                time.sleep(POLL_INTERVAL)

        print("Error during creation of node \
            Name: %s\n" % node_id)


