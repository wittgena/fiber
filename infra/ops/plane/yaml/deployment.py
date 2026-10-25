# fiber.infra.ops.plane.yaml.deployment
## @lineage: fiber.infra.watcher.plane.yaml.deployment
## @lineage: xphi.watcher.plane.infra.yaml.deployment
from typing import Dict

class KubeBlueprint:
    @staticmethod
    def generate_manifest(namespace: str, global_env: Dict[str, str], deploy_redis: bool = True) -> str:
        manifests = []
        
        # 1. Internal Redis Topology (Optional)
        if deploy_redis:
            redis_manifest = f"""\
apiVersion: v1
kind: Service
metadata:
  name: fiber-tunnel
  namespace: {namespace}
spec:
  selector:
    app: redis
  ports:
    - port: 6379
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fiber-tunnel
  namespace: {namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        readinessProbe:
          exec:
            command: ["redis-cli", "ping"]
          initialDelaySeconds: 2
          periodSeconds: 5
"""
            manifests.append(redis_manifest)

        # 2. Dynamic Environment Variables Rendering
        env_yaml_lines = []
        for k, v in global_env.items():
            env_yaml_lines.append(f"        - name: {k}")
            env_yaml_lines.append(f"          value: \"{v}\"")
        env_block = "\n".join(env_yaml_lines) if env_yaml_lines else "        []"

        # 3. Fiber Gateway (AI Agent Gateway) Topology
        gateway_manifest = f"""\
apiVersion: v1
kind: Service
metadata:
  name: fiber-gateway
  namespace: {namespace}
spec:
  selector:
    app: fiber-gateway
  ports:
    - port: 8356
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fiber-gateway
  namespace: {namespace}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fiber-gateway
  template:
    metadata:
      labels:
        app: fiber-gateway
    spec:
      volumes:
      - name: artifact-volume
        emptyDir: {{}}
      containers:
      - name: gateway
        image: fiber-node:local
        imagePullPolicy: Never
        command: ["fiber", "daemon", "-s", "rest_edge"]
        env:
{env_block}
        readinessProbe:
          httpGet:
            path: /health
            port: 8356
          initialDelaySeconds: 3
          periodSeconds: 5
        volumeMounts:
        - name: artifact-volume
          mountPath: /artifact_mount
"""
        manifests.append(gateway_manifest)
        
        return "\n---\n".join(manifests)