# Python Controller for Namespace Migration
from kubernetes import client, config, watch
import os
import time
from kubernetes.client.exceptions import ApiException

# Load config
try:
    config.load_incluster_config()
except:
    config.load_kube_config()

api = client.CustomObjectsApi()
core_api = client.CoreV1Api()
apps_api = client.AppsV1Api()

CRD_GROUP = "migrations.internal"
CRD_VERSION = "v1"
CRD_PLURAL = "namespacemigrations"


def wait_for_pv_available(pv_name, timeout=30):
    for _ in range(timeout):
        pv = core_api.read_persistent_volume(pv_name)
        if pv.status.phase == "Available":
            return True
        time.sleep(1)
    return False


def migrate_namespace(source_ns, target_ns):
    print(f"🚀 Starting migration from {source_ns} to {target_ns}")

    # Ensure target namespace exists
    try:
        core_api.create_namespace(
            client.V1Namespace(metadata=client.V1ObjectMeta(name=target_ns))
        )
        print(f"✅ Created namespace: {target_ns}")
    except ApiException as e:
        if e.status != 409:
            raise

    # Scale down StatefulSets in source
    for sts in apps_api.list_namespaced_stateful_set(source_ns).items:
        try:
            apps_api.patch_namespaced_stateful_set(
                sts.metadata.name,
                source_ns,
                {"spec": {"replicas": 0}},
            )
            print(f"🔽 Scaled down StatefulSet {sts.metadata.name}")
        except ApiException:
            pass

    time.sleep(5)

    # PVC Migration
    for pvc in core_api.list_namespaced_persistent_volume_claim(source_ns).items:

        if not pvc.spec.volume_name:
            print(f"⚠️ PVC {pvc.metadata.name} not bound, skipping")
            continue

        pv_name = pvc.spec.volume_name
        print(f"🔎 Migrating PVC {pvc.metadata.name} using PV {pv_name}")

        try:
            core_api.delete_namespaced_persistent_volume_claim(
                pvc.metadata.name, source_ns
            )
        except ApiException:
            pass

        time.sleep(2)

        try:
            core_api.patch_persistent_volume(
                pv_name,
                {"spec": {"claimRef": None}},
            )
        except ApiException:
            pass

        if not wait_for_pv_available(pv_name):
            print(f"❌ PV {pv_name} did not become Available")
            continue

        new_pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(name=pvc.metadata.name),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=pvc.spec.access_modes,
                resources=pvc.spec.resources,
                storage_class_name=pvc.spec.storage_class_name,
                volume_name=pv_name,
                volume_mode=pvc.spec.volume_mode,
            ),
        )

        try:
            core_api.create_namespaced_persistent_volume_claim(
                target_ns, new_pvc
            )
            print(f"✅ PVC {pvc.metadata.name} migrated")
        except ApiException as e:
            if e.status != 409:
                raise

    # ConfigMaps
    for cm in core_api.list_namespaced_config_map(source_ns).items:
        cm.metadata.namespace = target_ns
        cm.metadata.resource_version = None
        cm.metadata.uid = None
        try:
            core_api.create_namespaced_config_map(target_ns, cm)
            print(f"✅ ConfigMap {cm.metadata.name} migrated")
        except ApiException:
            pass

    # Secrets
    for secret in core_api.list_namespaced_secret(source_ns).items:
        secret.metadata.namespace = target_ns
        secret.metadata.resource_version = None
        secret.metadata.uid = None
        try:
            core_api.create_namespaced_secret(target_ns, secret)
            print(f"✅ Secret {secret.metadata.name} migrated")
        except ApiException:
            pass

    # Deployments
    for deploy in apps_api.list_namespaced_deployment(source_ns).items:
        deploy.metadata.namespace = target_ns
        deploy.metadata.resource_version = None
        deploy.metadata.uid = None
        try:
            apps_api.create_namespaced_deployment(target_ns, deploy)
            print(f"✅ Deployment {deploy.metadata.name} migrated")
        except ApiException:
            pass

        try:
            apps_api.delete_namespaced_deployment(deploy.metadata.name, source_ns)
            print(f"🗑️ Deployment {deploy.metadata.name} removed from source")
        except ApiException:
            pass

    # StatefulSets
    for sts in apps_api.list_namespaced_stateful_set(source_ns).items:
        sts.metadata.namespace = target_ns
        sts.metadata.resource_version = None
        sts.metadata.uid = None
        sts.spec.replicas = 1
        try:
            apps_api.create_namespaced_stateful_set(target_ns, sts)
            print(f"✅ StatefulSet {sts.metadata.name} migrated")
        except ApiException:
            pass

        try:
            apps_api.delete_namespaced_stateful_set(sts.metadata.name, source_ns)
            print(f"🗑️ StatefulSet {sts.metadata.name} removed from source")
        except ApiException:
            pass

    print("🎉 Migration completed successfully")


def watch_migrations():
    w = watch.Watch()
    print("👀 Watching for namespace migration CRDs...")
    for event in w.stream(
        api.list_cluster_custom_object,
        CRD_GROUP,
        CRD_VERSION,
        CRD_PLURAL,
    ):
        migration = event["object"]
        source_ns = migration["spec"]["sourceNamespace"]
        target_ns = migration["spec"]["targetNamespace"]
        migrate_namespace(source_ns, target_ns)


if __name__ == "__main__":
    watch_migrations()
