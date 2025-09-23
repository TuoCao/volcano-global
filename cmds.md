
```shell

kubectl --context karmada-apiserver apply -f 

make volcano-global-controller-manager-image
kubectl --context karmada-host delete -f docs/deploy/volcano-global-controller-manager.yaml
docker exec -it karmada-host-control-plane crictl rmi docker.io/volcanosh/volcano-global-controller-manager:latest
kind load docker-image volcanosh/volcano-global-controller-manager:latest --name karmada-host  
kubectl --context karmada-host apply -f docs/deploy/volcano-global-controller-manager.yaml
kubectl --context karmada-host get pod -A 
kubectl --context karmada-host -n volcano-global logs


kubectl --context karmada-host delete -f docs/deploy/volcano-global-controller-manager.yaml
docker exec -it karmada-host-control-plane crictl rmi docker.io/volcanosh/volcano-global-controller-manager:latest
kind load docker-image volcanosh/volcano-global-controller-manager:latest --name karmada-host  
kubectl --context karmada-host apply -f docs/deploy/volcano-global-controller-manager.yaml


kubectl --context karmada-host delete -f docs/deploy/volcano-global-webhook-manager.yaml
docker exec -it karmada-host-control-plane crictl rmi docker.io/volcanosh/volcano-global-webhook-manager:latest
kind load docker-image volcanosh/volcano-global-webhook-manager:latest --name karmada-host  
kubectl --context karmada-host apply -f docs/deploy/volcano-global-webhook-manager.yaml


export KUBECONFIG=$HOME/.kube/members.config

kubectl --context member1 apply -f workloads/ranktable-test/configmap-1.yaml

kubectl --context karmada-apiserver apply -f  workloads/ranktable-test/global-ranktable.yaml
```

