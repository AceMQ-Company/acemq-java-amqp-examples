#!/usr/bin/env bash
# Generate throwaway certificates for local development.
#
#   ./generate-dev-certs.sh [output-directory]        (default: ./certs)
#
# Produces what both sides need:
#
#   ca.crt  ca.key            a certificate authority, trusted by nobody else
#   server.crt  server.key    for RabbitMQ, CN=localhost, signed by that CA
#   truststore.p12           for AceMQ: the CA it should accept
#   keystore.p12             for AceMQ: this client's own key pair
#
# Every certificate carries "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST" in its
# subject. AceMQ refuses such a certificate unless the policy explicitly calls
# allowDevelopmentCertificates(), so one of these reaching production fails
# closed rather than quietly encrypting nothing worth trusting.
#
# This is what the library's own certificate generator will do when it exists.
# It is not built yet: the JDK produces key pairs rather than certificates, so
# signing needs a dependency like BouncyCastle. openssl already does the job.
set -euo pipefail

OUT="${1:-certs}"
MARKER="ACEMQ DEVELOPMENT ONLY - DO NOT TRUST"
# Six characters minimum: keytool refuses to create a PKCS12 keystore with a
# shorter password, and AceMQ's built-in default ("acemq") is five. The example
# passes this to keystorePassword(...) rather than relying on the default.
PASSWORD="acemq-dev"
DAYS=90                   # short on purpose: a development certificate should expire

command -v openssl >/dev/null || { echo "openssl is required" >&2; exit 1; }
command -v keytool >/dev/null || { echo "keytool is required (it ships with the JDK)" >&2; exit 1; }

mkdir -p "$OUT"
cd "$OUT"

echo "==> certificate authority"
openssl req -x509 -newkey rsa:4096 -sha256 -days "$DAYS" -nodes \
  -keyout ca.key -out ca.crt \
  -subj "/CN=AceMQ development CA/OU=$MARKER" 2>/dev/null

echo "==> server certificate for localhost"
openssl req -newkey rsa:4096 -sha256 -nodes \
  -keyout server.key -out server.csr \
  -subj "/CN=localhost/OU=$MARKER" 2>/dev/null

# A subject alternative name, because hostname verification ignores the common
# name. Without this the certificate verifies as valid and is still rejected for
# the host, which is a confusing way to spend an afternoon.
cat > server.ext <<EXT
subjectAltName = DNS:localhost, DNS:rabbitmq, IP:127.0.0.1
extendedKeyUsage = serverAuth
EXT

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$DAYS" -sha256 -extfile server.ext 2>/dev/null

echo "==> client key pair"
openssl req -newkey rsa:4096 -sha256 -nodes \
  -keyout client.key -out client.csr \
  -subj "/CN=acemq-client/OU=$MARKER" 2>/dev/null
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days "$DAYS" -sha256 2>/dev/null

echo "==> keystore.p12 and truststore.p12"
openssl pkcs12 -export -in client.crt -inkey client.key -certfile ca.crt \
  -name acemq-client -out keystore.p12 -passout "pass:$PASSWORD" 2>/dev/null

rm -f truststore.p12
keytool -importcert -noprompt -alias acemq-dev-ca -file ca.crt \
  -keystore truststore.p12 -storetype PKCS12 -storepass "$PASSWORD" >/dev/null

chmod 600 ./*.key ./*.p12
rm -f server.csr client.csr server.ext ca.srl

echo
echo "Written to $(pwd):"
ls -1 ca.crt server.crt server.key keystore.p12 truststore.p12
echo
echo "These expire in $DAYS days and are trusted by nothing but this directory."
