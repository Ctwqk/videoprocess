# macOS Offload Deploy Scripts

This directory holds the repo-local implementation scripts for services that are
developed from the Linux host and deployed to the offloaded macOS machines.

## Official Entrypoint

Normal deploy entrypoint:

- `/home/taiwei/k8s-Constructure/k8s-constructure/scripts/deploy-offloaded-services.sh`

Use that script for routine deploys. It is the canonical cluster-level command.

## Script Roles

- `common.sh`
  - shared SSH, rsync, runtime-install, and path helpers
- `deploy_videoprocess_workers.sh`
  - installs and restarts host-native `vp-worker` on Mac 1
- `deploy_videoprocess_everywhere.sh`
  - repo-local convenience wrapper for VideoProcess worker rollout across K8s +
    Mac 1
- `deploy_news_stack.sh`
  - installs and restarts `embedding-gateway`, `news-server`, and
    `news-collector` on Mac 3
- `offload_to_macs.sh`
  - compatibility wrapper that forwards to the canonical cluster-level script

## Policy

- keep `offload_to_macs.sh` only as a compatibility layer
- keep the scripts in this directory as repo-local implementation details
- start normal deploys from `~/k8s-Constructure/k8s-constructure`

## Typical Commands

```bash
cd /home/taiwei/k8s-Constructure/k8s-constructure

./scripts/deploy-offloaded-services.sh all
./scripts/deploy-offloaded-services.sh videoprocess
./scripts/deploy-offloaded-services.sh news
```
