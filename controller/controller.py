# Python Controller for Namespace Migration
from kubernetes import client, config, watch
import os
import time
from kubernetes.client.exceptions import ApiException

# Load config
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


def migrate_namespace(source_ns, target_ns):
    print(f"🚀 Starting migration from {source_ns} to {target_ns}")

    try:
        core_api.create_namespace(
            client.V1Namespace(metadata=client.V1ObjectMeta(name=target_ns))
        )
        print(f"✅ Created namespace: {target_ns}")
    except ApiException as e:
        if e.status != 409:
            raise

    # 1️⃣ Scale down StatefulSets in source
    for sts in apps_api.list_namespaced_stateful_set(source_ns).items:
        apps_api.patch_namespaced_stateful_set(
            sts.metadata.name,
            source_ns,
            {"spec": {"replicas": 0}},
        )
        print(f"🔽 Scaled down StatefulSet {sts.metadata.name}")

    time.sleep(5)

    # 2️⃣ Migrate PVCs
    for pvc in core_api.list_namespaced_persistent_volume_claim(source_ns).items:
        pv_name = pvc.spec.volume_name
        print(f"🔎 Migrating PVC {pvc.metadata.name} using PV {pv_name}")

        core_api.delete_namespaced_persistent_volume_claim(
            pvc.metadata.name, source_ns
        )

        time.sleep(2)

        core_api.patch_persistent_volume(
            pv_name,
            {"spec": {"claimRef": None}},
        )

        time.sleep(2)

        new_pvc = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(name=pvc.metadata.name),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=pvc.spec.access_modes,
                resources=pvc.spec.resources,
                storage_class_name="",
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
            if e.status == 409:
                print(f"⚠️ PVC {pvc.metadata.name} already exists, skipping")
            else:
                raise

    # 3️⃣ Migrate ConfigMaps
    for cm in core_api.list_namespaced_config_map(source_ns).items:
        cm.metadata.namespace = target_ns
        cm.metadata.resource_version = None
        cm.metadata.uid = None
        try:
            core_api.create_namespaced_config_map(target_ns, cm)
            print(f"✅ ConfigMap {cm.metadata.name} migrated")
        except ApiException as e:
            if e.status == 409:
                print(f"⚠️ ConfigMap {cm.metadata.name} already exists, skipping")
            else:
                raise

    # 4️⃣ Migrate Secrets
    for secret in core_api.list_namespaced_secret(source_ns).items:
        secret.metadata.namespace = target_ns
        secret.metadata.resource_version = None
        secret.metadata.uid = None
        try:
            core_api.create_namespaced_secret(target_ns, secret)
            print(f"✅ Secret {secret.metadata.name} migrated")
        except ApiException as e:
            if e.status == 409:
                print(f"⚠️ Secret {secret.metadata.name} already exists, skipping")
            else:
                raise

    # 5️⃣ Migrate Deployments
    for deploy in apps_api.list_namespaced_deployment(source_ns).items:
        deploy.metadata.namespace = target_ns
        deploy.metadata.resource_version = None
        deploy.metadata.uid = None
        try:
            apps_api.create_namespaced_deployment(target_ns, deploy)
            print(f"✅ Deployment {deploy.metadata.name} migrated")
        except ApiException as e:
            if e.status == 409:
                print(f"⚠️ Deployment {deploy.metadata.name} already exists, skipping")
            else:
                raise

    # 6️⃣ Migrate StatefulSets
    for sts in apps_api.list_namespaced_stateful_set(source_ns).items:
        sts.metadata.namespace = target_ns
        sts.metadata.resource_version = None
        sts.metadata.uid = None
        sts.spec.replicas = 1
        try:
            apps_api.create_namespaced_stateful_set(target_ns, sts)
            print(f"✅ StatefulSet {sts.metadata.name} migrated")
        except ApiException as e:
            if e.status == 409:
                print(f"⚠️ StatefulSet {sts.metadata.name} already exists, skipping")
            else:
                raise

    print("🎉 Migration completed (source namespace preserved)")


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
