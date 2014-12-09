#!/bin/bash
#
# Based on finagle-zookeeper's integration tests
#

if [[ $# -ne 1 ]]; then
    echo "Usage: stop.sh <version>"
    exit 1
fi

CWD=$(pwd)
BIN=$CWD/bin
DATADIR=$CWD/data
VERSION=$1

is_up() {
    local port=$1

    echo mntr | nc localhost ${port} 2> /dev/null | grep standalone > /dev/null
}

>&2 echo "Stopping server v${VERSION}"
cd $BIN/zookeeper/bin/
./zkServer.sh stop

while :; do
    is_up

    if [[ $? -ne 0 ]]; then
        break
    fi

    sleep 0.5
done

>&2 echo "Removing temp directories"
rm -rf $BIN
rm -rf $DATADIR
