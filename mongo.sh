DATA_DIR=~/klaytn/db

function start {
  # TODO-WINNIE check auth and ssl setting
  # TODO-WINNIE check compression options
  if [ $(echo 'db.runCommand("ping").ok' | mongo localhost:27017 --quiet | head -n 1 | awk '{print $1;}' ) != "1" ]; then
    echo "start mongoDB"
    if [ ! -d $DATA_DIR ]; then
      mkdir -p $DATA_DIR
    fi
    mongod --storageEngine wiredTiger --dbpath $DATA_DIR &
  fi
}

function stop {
  echo 'db.adminCommand({   shutdown: 1,   force: false,   timeoutSecs: 10 })' | mongo localhost:27017 --quiet
  rm -rf $DATA_DIR/*
}

function main {
  $1
}

main "$@"
