#!/bin/bash
set -eu
mkdir -p vpn-certs
docker run -it --rm -v $PWD/vpn-certs:/vpn-certs -v $PWD/util:/util ubuntu:18.04 /util/generate-certs-docker.sh
echo "VPN certificates generated in $PWD/vpn-certs directory."
echo "| caKey.pem       | CA root certificate."
echo "| caCert.pem      | CA certificate for VPN Gateway. Imported by Terraform."
echo "| caCert.txt      | CA certificate for VPN Gateway, data as text. Imported by Terraform."
echo "| clientKey.pem   | Private key for client."
echo "| clientCert.pem  | CA certificate for private key for client."
echo "| client.p12      | CA signed private key for client. Import it on your client machine. Find the user and password in the script in util/ directory."
