#!/bin/sh
#
# Based on finagle-zookeeper's integration tests
#

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
    cp build/zookeeper-$VERSION.jar zookeeper-$VERSION.jar
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

while :; do
    is_up ${PORT}

    if [[ $? -eq 0 ]]; then
        break;
    fi

    sleep 0.5
done
