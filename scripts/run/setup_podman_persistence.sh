#!/usr/bin/env bash
#
# One-time VM setup so the OMOP DB container survives a reboot without
# anyone needing to be logged in first. This needs to be re-run for a
# ny account using the DB container
#
#   scripts/run/setup_podman_persistence.sh
#
# Both steps are needed:
# - `loginctl enable-linger` makes systemd start this user's --user systemd
#   session at boot, independent of anyone actually logging in. Without
#   this, rootless podman containers can be killed the moment the last SSH
#   session for this user ends, and nothing runs again at boot at all
#   (`loginctl show-user $USER --property=Linger` was `Linger=no`).
# - `podman-restart.service` is what revives any container with a
#   --restart policy (run_db_podman.sh already sets --restart=always) once
#   that session comes up (also disabled on the VM).

set -euo pipefail

echo "==> enabling lingering for $(whoami)"
loginctl enable-linger "$(whoami)"

echo "==> enabling podman-restart.service (user scope)"
systemctl --user enable --now podman-restart.service

echo "==> verifying"
loginctl show-user "$(whoami)" --property=Linger
systemctl --user is-enabled podman-restart.service

echo "==> done: the DB container will survive a VM reboot with nobody logged in"