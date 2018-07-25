from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from os import path
import yaml
import random

import kubernetes.client as k8sclient
import kubernetes.config as k8sconfig

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME

def to_k8s_format(tags):
    """ Convert ray node name tags to kubernetes pod labels """

def from_k8s_format(tags):
    """ Convert kubernetes pod labels to ray node name tags """

class KubernetesNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        print(provider_config) 
        # Extract necessary fields from provider config
        dep = provider_config["deployments"]
        self.namespace = provider_config["namespace"]
        svc = provider_config["services"]
        # Load kube config
        k8sconfig.load_kube_config()

        # Initialize client api objects
        self.client_v1 = k8sclient.CoreV1Api()
        self.client_appsv1beta1 = k8sclient.AppsV1beta1Api() 
        
        # Create service for Redis server
        svc_resp = self.client_v1.create_namespaced_service(
            namespace=self.namespace,
            body=svc["redis-service"]
            )

        # Deploy ray head
        head_resp = self.client_appsv1beta1.create_namespaced_deployment(
            namespace=self.namespace,
            body=dep["worker"]
            )

        print("Deployment created. status='%s'" % str(resp.status))

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_nodes = {}

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
        raise NotImplementedError

    def is_running(self, node_id):
        """Return whether the specified node is running."""
        raise NotImplementedError

    def is_terminated(self, node_id):
        """Return whether the specified node is terminated."""
        raise NotImplementedError

    def node_tags(self, node_id):
        """Returns the tags of the given node (string dict)."""
        raise NotImplementedError

    def external_ip(self, node_id):
        """Returns the external ip of the given node."""
        raise NotImplementedError

    def internal_ip(self, node_id):
        """Returns the internal ip (Ray ip) of the given node."""
        raise NotImplementedError

    def create_node(self, node_config, tags, count):
        """Creates a number of nodes within the namespace."""
        raise NotImplementedError

    def set_node_tags(self, node_id, tags):
        """Sets the tag values (string dict) for the specified node."""
        raise NotImplementedError

    def terminate_node(self, node_id):
        """Terminates the specified node."""
        raise NotImplementedError
