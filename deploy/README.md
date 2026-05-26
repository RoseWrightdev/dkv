# Usage Guide

## 1. Building the Docker Image

To build the dkv container image, run the following command from the root of the project:

```bash
docker build -t dkv:latest -f deploy/Dockerfile .
```

To run it locally in Docker for development purposes:

```bash
docker run -p 50051:50051 -p 7946:7946 dkv:latest
```

---

## 2. Deploying to Kubernetes with Helm

### Installation

To install the chart in your Kubernetes cluster under the release name `dkv`:

```bash
helm install dkv ./deploy/charts/dkv
```

### Upgrade / Configuration

To upgrade your chart or tweak properties:

```bash
helm upgrade dkv ./deploy/charts/dkv -f deploy/charts/dkv/values.yaml
```

To uninstall and clean up all resources:

```bash
helm uninstall dkv
```

---

## 3. Client Connection Guide inside Kubernetes

Once deployed, the chart creates two key services in your namespace:
1. ClusterIP Service (`dkv`): Provides single-point load balancing over the cluster.
2. Headless Service (`dkv-headless`): Resolves directly to the individual replica IPs (useful for cluster discovery, client-side ring hashing, or direct target writes).

### Python Client Integration

If your microservice is running inside the same Kubernetes namespace, you can connect directly to the service using the `dkv` Python client.

#### A. Connect to the Load-Balanced Service
Use this for simple read/write workloads where standard Kubernetes load balancing is sufficient.

```python
from dkv import DKVClient, insecure_credentials

# Standard Kubernetes service DNS resolving to the ClusterIP
address = "dkv.default.svc.cluster.local:50051"

with DKVClient.connect(address, insecure_credentials()) as client:
    client.set("foo", b"bar")
    value = client.get("foo")
    print(f"Retrieved: {value}")
```

#### B. Connect to a Specific StatefulSet Pod (Direct Sharding/Addressing)
For master-replica writes, sharding, or specific debugging, you can direct traffic to individual pods (`dkv-0`, `dkv-1`, `dkv-2`) using the headless service DNS.

```python
from dkv import DKVClient, insecure_credentials

# Explicitly address pod 0 directly
address = "dkv-0.dkv-headless.default.svc.cluster.local:50051"

with DKVClient.connect(address, insecure_credentials()) as client:
    client.set("pod-specific-key", b"direct-write")
```
