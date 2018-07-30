from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import yaml
import random

import kubernetes.client as k8sclient
import kubernetes.config as k8sconfig

from k8sclient.rest import ApiException

from ray.autoscaler.node_provider import NodeProvider, DEFAULT_CONFIGS
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME

class KubernetesNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

        # Set namespace this cluster is running in
        self.namespace = provider_config["namespace"]

        if "KUBERNETES_SERVICE_HOST" in os.environ:
            # We're already in the k8s pod here
            # Load in cluster config
            k8sconfig.load_incluster_config()

            # Initialize client api objects
            self.client_v1 = k8sclient.CoreV1Api()
            self.client_appsv1 = k8sclient.AppsV1beta1Api()

        else:
            # We're off cluster, we need to start a k8s cluster
            try:
                # Load the kube config
                k8sconfig.load_kube_config()

                # Initialize client api objects
                self.client_v1 = k8sclient.CoreV1Api()
                self.client_appsv1 = k8sclient.AppsV1beta1Api()
                
            except Exception as e:
                # No k8s service or incorrectly configured
                print("Exception trying load kube config: %s\n" % e)
                return

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_pods = {}
        self.cached_deployments = {}

        # Cache of ip lookups. We assume IPs never change once assigned.
        self.internal_ip_cache = {}
        self.external_ip_cache = {}

    def nodes(self, tag_filter):
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
            for (k,v) in tag_filter.items()
        ])
        print(label_selector)
        # Make call to the k8s master having applied the filters
        try:    
            # Get pods
            pod_list = self.client_v1.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector)

            # Get deployments
            deployment_list = self.client_appsv1.list_namespaced_deployment(namespace=self.namespace)
        
        except ApiException as e:
            print("Exception when listing pods or deployments: %s\n" % e)

        # Now apply the status filters
        pods = [pod for pod in pod_list.items if pod.status.phase in ["Running", "Pending"]]
        # Cache pods and available deployments
        self.cached_pods = {pod.metadata.name:pod for pod in pods}
        self.cached_deployments = {dep.metadata.name:dep for dep in deployment_list.items}

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
        return node.status.host_ip

    def internal_ip(self, node_id):
        """Returns the internal ip (Ray ip) of the given node."""
        node = self._node(node_id)
        return node.status.pod_ip
        
    def create_node(self, node_config, tags, count):
        """Creates a number of nodes within the namespace."""

        dep_body = node_config["deployments"]
        svc_body = node_config["services"]
        dep_name = dep_body["metadata"]["name"]
        
        # First check whether a deployment of the same name exists
        if dep_name in self.cached_deployments:
            # Such a deployment already exists, just scale it
            num_replicas = self.cached_deployments[dep_name].status.available_replicas + count
            try:    
                self.client_appsv1.patch_namespaced_deployment(
                    namespace=self.namespace,
                    body={
                    "metadata": {"labels": tags},
                    "spec": {"replicas": num_replicas}
                    })
            except ApiException as e:
                print("Exception when trying to scale up %s to %d pods : %s" % (dep_name, num_replicas, e))
        else:
            # No such deployment exists, fill in tags
            dep_body["metadata"]["labels"] = tags
            try:    
                # Now create a new deployment
                self.client_appsv1.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=dep_body)
            except ApiException as e:
                print("Error trying to create deployment %s: %s" % (dep_name,e))

        # If a service needs to be created do it here
        if svc_body:
            try:
                self.client_v1.create_namespaced_service(
                    namespace=self.namespace,
                    body=svc_body
                    )
            except ApiException as e:
                print("Error trying to create service %s: %s" % (svc_body["metadata"]["name"],e))


    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        node = self._node(node_id)
        # Create the k8s patch body
        patch = {
        "metadata" : {
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
        if self

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

