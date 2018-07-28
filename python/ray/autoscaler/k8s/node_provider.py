from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import yaml
import random

import kubernetes.client as k8sclient
import kubernetes.config as k8sconfig

from ray.autoscaler.node_provider import NodeProvider, DEFAULT_CONFIGS
from ray.autoscaler.tags import TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_NAME


class KubernetesNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        
        # Hack until schema specifications are reformatted for k8s
        default_config_loader = DEFAULT_CONFIGS.get("kubernetes")
        config_path = default_config_loader()
        with open(config_path) as f:
            default_config = yaml.load(f)
            provider_config = default_config["provider"]
        
        # Extract necessary fields from provider config
        dep = provider_config["deployments"]
        self.namespace = provider_config["namespace"]
        svc = provider_config["services"]
    

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

                # Deploy a ray head 
                head_resp = self.client_appsv1.create_namespaced_deployment(
                    namespace=self.namespace,
                    body=dep["head"]
                    )
                 # Create service for Redis server
                svc_resp = self.client_v1.create_namespaced_service(
                    namespace=self.namespace,
                    body=svc["redis-service"]
                    )
                
            except:
                # No k8s service or incorrectly configured
                return

        # Deploy ray worker
        worker_resp = self.client_appsv1.create_namespaced_deployment(
            namespace=self.namespace,
            body=dep["worker"]
            )

        # Cache of node objects from the last nodes() call. This avoids
        # excessive DescribeInstances requests.
        self.cached_pods = []

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
        v1_pod_list = self.client_v1.list_namespaced_pod(namespace=self.namespace, label_selector=label_selector)
        # Now apply the status filters
        pods = [pod for pod in v1_pod_list.items if pod.status.phase in ["Running", "Pending"]]
        # Cache pods
        self.cached_pods = {pod.metadata.name:pod for pod in pods}

        return [pod.metadata.name for pod in pods]
    
    def is_running(self, node_id):
        """Return whether the specified node is running."""
        

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
