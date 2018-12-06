#!/usr/bin/env bash
/usr/bin/getent group bioyino || /usr/sbin/groupadd -r bioyino
/usr/bin/getent passwd bioyino || /usr/sbin/useradd -d /tmp -M -s /bin/false --system -g bioyino bioyino
[[ -e /bin/systemctl ]] && /bin/systemctl daemon-reload ||:
