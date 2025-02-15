# Python Controller for Namespace Migration
from kubernetes import client, config, watch
import os
from kubernetes.client.exceptions import ApiException

# Load Kubeconfig (inside cluster or local)
if "KUBERNETES_SERVICE_HOST" in os.environ:
    config.load_incluster_config()
else:
    config.load_kube_config()

api = client.CustomObjectsApi()
core_api = client.CoreV1Api()
apps_api = client.AppsV1Api()

CRD_GROUP = "migrations.internal"
CRD_VERSION = "v1"
CRD_PLURAL = "namespacemigrations"

def delete_namespace_resources(namespace):
    """Delete all resources in the namespace before deleting it."""
    print(f"🗑️ Cleaning up resources in namespace: {namespace}")

    # Delete Deployments
    for deploy in apps_api.list_namespaced_deployment(namespace).items:
        apps_api.delete_namespaced_deployment(deploy.metadata.name, namespace)
        print(f"✅ Deleted Deployment: {deploy.metadata.name}")

    # Delete StatefulSets
    for sts in apps_api.list_namespaced_stateful_set(namespace).items:
        apps_api.delete_namespaced_stateful_set(sts.metadata.name, namespace)
        print(f"✅ Deleted StatefulSet: {sts.metadata.name}")

    # Delete Pods
    for pod in core_api.list_namespaced_pod(namespace).items:
        core_api.delete_namespaced_pod(pod.metadata.name, namespace)
        print(f"✅ Deleted Pod: {pod.metadata.name}")

    # Delete Persistent Volume Claims (PVCs)
    for pvc in core_api.list_namespaced_persistent_volume_claim(namespace).items:
        core_api.delete_namespaced_persistent_volume_claim(pvc.metadata.name, namespace)
        print(f"✅ Deleted PVC: {pvc.metadata.name}")

    # Delete ConfigMaps
    for cm in core_api.list_namespaced_config_map(namespace).items:
        core_api.delete_namespaced_config_map(cm.metadata.name, namespace)
        print(f"✅ Deleted ConfigMap: {cm.metadata.name}")

    # Delete Secrets
    for secret in core_api.list_namespaced_secret(namespace).items:
        core_api.delete_namespaced_secret(secret.metadata.name, namespace)
        print(f"✅ Deleted Secret: {secret.metadata.name}")

    print(f"✅ All resources deleted from namespace {namespace}")

    # Delete the namespace
    try:
        core_api.delete_namespace(namespace)
        print(f"✅ Namespace {namespace} deleted successfully.")
    except ApiException as e:
        print(f"❌ Error deleting namespace {namespace}: {e}")

def migrate_namespace(source_ns, target_ns):
    print(f"🚀 Starting migration from {source_ns} to {target_ns}...")

    # Ensure target namespace exists
    try:
        core_api.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=target_ns)))
        print(f"✅ Created namespace: {target_ns}")
    except ApiException as e:
        if e.status == 409:
            print(f"⚠️ Namespace {target_ns} already exists, skipping creation.")
        else:
            raise

    # Migrate Deployments
    for deploy in apps_api.list_namespaced_deployment(source_ns).items:
        deploy.metadata.namespace = target_ns
        deploy.metadata.resource_version = None
        try:
            apps_api.create_namespaced_deployment(target_ns, deploy)
            print(f"✅ Deployment {deploy.metadata.name} migrated successfully.")
        except ApiException as e:
            print(f"❌ Error migrating Deployment {deploy.metadata.name}: {e}")

    # Migrate StatefulSets
    for sts in apps_api.list_namespaced_stateful_set(source_ns).items:
        sts.metadata.namespace = target_ns
        sts.metadata.resource_version = None
        try:
            apps_api.create_namespaced_stateful_set(target_ns, sts)
            print(f"✅ StatefulSet {sts.metadata.name} migrated successfully.")
        except ApiException as e:
            print(f"❌ Error migrating StatefulSet {sts.metadata.name}: {e}")

    # Migrate ConfigMaps
    for cm in core_api.list_namespaced_config_map(source_ns).items:
        cm.metadata.namespace = target_ns
        cm.metadata.resource_version = None
        try:
            core_api.create_namespaced_config_map(target_ns, cm)
            print(f"✅ ConfigMap {cm.metadata.name} migrated successfully.")
        except ApiException as e:
            print(f"❌ Error migrating ConfigMap {cm.metadata.name}: {e}")

    # Migrate Secrets
    for secret in core_api.list_namespaced_secret(source_ns).items:
        secret.metadata.namespace = target_ns
        secret.metadata.resource_version = None
        try:
            core_api.create_namespaced_secret(target_ns, secret)
            print(f"✅ Secret {secret.metadata.name} migrated successfully.")
        except ApiException as e:
            print(f"❌ Error migrating Secret {secret.metadata.name}: {e}")

    # Cleanup old namespace
    delete_namespace_resources(source_ns)

    print(f"🎉 Migration completed from {source_ns} to {target_ns}")

def watch_migrations():
    w = watch.Watch()
    print("👀 Watching for namespace migration CRDs...")
    for event in w.stream(api.list_cluster_custom_object, CRD_GROUP, CRD_VERSION, CRD_PLURAL):
        migration = event['object']
        source_ns = migration['spec']['sourceNamespace']
        target_ns = migration['spec']['targetNamespace']
        migrate_namespace(source_ns, target_ns)

if __name__ == "__main__":
    watch_migrations()
