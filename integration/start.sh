#!/bin/bash
#
# Based on finagle-zookeeper's integration tests
#

set -eux

if [[ $# -ne 3 ]]; then
    echo "Usage: start.sh <version> <url> <port>"
    exit 1
fi

VERSION=$1
URL=$2
PORT=$3

CWD=$(pwd)
BIN=$CWD/bin
DATADIR=$CWD/data
RELEASEDIR=$CWD/releases

safe_mkdir() {
    local dir=$1

    if [[ ! -d ${dir} ]]; then
        mkdir -p ${dir}
    fi
}

safe_rmdir() {
    local dir=$1

    if [[ -d ${dir} ]]; then
        rm -rf ${dir}
    fi
}

is_up() {
    local port=$1

    echo mntr | nc localhost ${port} 2> /dev/null | grep standalone > /dev/null
}

>&2 echo "Installing Zookeeper v${VERSION}"
safe_mkdir $BIN
safe_mkdir $DATADIR
safe_mkdir $RELEASEDIR

cd $RELEASEDIR

if [[ ! -f $RELEASEDIR/release-$VERSION.tar.gz ]]; then
    wget ${URL}
    safe_rmdir zookeeper-$VERSION
fi

if [[ ! -d zookeeper-$VERSION ]]; then
    tar -zxf release-$VERSION.tar.gz
    mv zookeeper-release-$VERSION zookeeper-$VERSION
    cd zookeeper-$VERSION/
    ant package
    if [ -e build/zookeeper-$VERSION.jar ]; then
      BUILT_ZOOKEEPER_JAR=build/zookeeper-$VERSION.jar
    elif [ -e build/zookeeper-$VERSION-alpha.jar ]; then
      BUILT_ZOOKEEPER_JAR=build/zookeeper-$VERSION-alpha.jar
    else
      echo 'Could not find a ZooKeeper jar.'
      exit 1
    fi
    cp $BUILT_ZOOKEEPER_JAR zookeeper-$VERSION.jar
    cp -R build/lib/ lib
fi

cd $RELEASEDIR/zookeeper-$VERSION/
cp -R ../zookeeper-$VERSION $BIN/zookeeper
chmod a+x $BIN/zookeeper/bin/zkServer.sh
cd $BIN/zookeeper/conf
cat <<EOF > zoo.cfg
tickTime=2000
initLimit=10
syncLimit=5
dataDir=$DATADIR
clientPort=2181
EOF
>&2 echo "Finished installing v${VERSION}"
>&2 echo "Starting Zookeeper v${VERSION}"
cd $BIN/zookeeper/bin/
./zkServer.sh start

sleep 10

is_up ${PORT}
echo "ZooKeeper is up!"
