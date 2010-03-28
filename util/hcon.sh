#!/bin/bash -e

# need to replace this with the fully qualified path to your private key file
# for emr.
KEYFILE="/Users/pskomoroch/id_rsa-gsg-keypair"

# assumes 'open' (mac os).  otherwise you can set the BROWSER env variable
# or just change this line..
if [ -z "${BROWSER}" ]; then
  BROWSER="open"
fi

if [ $# -lt 1 ]; then
  echo "$0 <host>";
  echo "e.g. $0 ec2-XXX-XXX-XXX-XXX.compute-1.amazonaws.com";
  exit 1;
fi

if [ "PATH_TO_YOUR_KEY_FILE" = "${KEYFILE}" ]; then
  echo "you need to configure the KEYFILE variable.";
  exit 1;
fi

if [ ! -f $KEYFILE ]; then
  echo "your KEYFILE:${KEYFILE} does not exist.";
  exit 1;
fi

HOST=$1; shift;
LPORT=$[ ( $RANDOM % ( $[ 70000 - 10000 ] + 1 ) ) + 10000 ]

ssh -f -N -o "StrictHostKeyChecking no" -L ${LPORT}:localhost:9100 -i ${KEYFILE} hadoop@${HOST}
$BROWSER http://localhost:${LPORT}
