# macOS Offload Deploy Scripts

This directory holds the repo-local implementation scripts for services that are
developed from the Linux host and deployed to the offloaded macOS machines.

## Active VideoProcess Topology

- `10.0.0.150`: Swarm manager, shared Postgres/Redis/MinIO, deploy controller,
  and managed Python worker.
- `10.0.0.127`: primary VideoProcess Colima/Swarm application and Go-worker
  node.
- `10.0.0.126`: ForWin/news node; it does not participate in normal
  VideoProcess deployment or failover.

Restore or inspect the 127 node with:

```bash
./install_videoprocess_colima_node.sh doctor
./install_videoprocess_colima_node.sh install
./install_videoprocess_colima_node.sh status
```

## Official Entrypoint

Normal deploy entrypoint:

- `/home/taiwei/k8s-Constructure/k8s-constructure/scripts/deploy-offloaded-services.sh`

Use that script for routine deploys. It is the canonical cluster-level command.

For the active GitHub-to-Swarm path on 150, deploy only VideoProcess projects:

```bash
/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh \
  --apply --project vp-app --project vp-feature-aggregator
```

Run the command manually for a new deployment before enabling the equivalent
VP-only cron entry. Do not use an unscoped deploy-sync invocation for a
VideoProcess-only release.

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
