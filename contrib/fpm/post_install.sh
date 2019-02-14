#!/usr/bin/env bash
/usr/bin/getent group bioyino || /usr/sbin/groupadd -r bioyino >/dev/null
/usr/bin/getent passwd bioyino || /usr/sbin/useradd -d /tmp -M -s /bin/false --system -g bioyino bioyino >/dev/null
[[ -e /bin/systemctl ]] && /bin/systemctl daemon-reload ||:
