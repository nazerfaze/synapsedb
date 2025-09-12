#!/bin/bash
# SSL Certificate Generation Script for SynapseDB
set -e

SSL_DIR="/var/lib/postgresql/ssl"
NODE_NAME="${SYNAPSEDB_NODE_NAME:-node1}"
CLUSTER_NAME="${SYNAPSEDB_CLUSTER_NAME:-synapsedb}"

# Create SSL directory
mkdir -p "$SSL_DIR"
cd "$SSL_DIR"

echo "Generating SSL certificates for $NODE_NAME in cluster $CLUSTER_NAME"

# Generate CA private key
if [ ! -f ca.key ]; then
    openssl genrsa -out ca.key 4096
    echo "Generated CA private key"
fi

# Generate CA certificate
if [ ! -f ca.crt ]; then
    openssl req -new -x509 -key ca.key -days 3650 -out ca.crt -subj "/C=US/ST=CA/L=SF/O=SynapseDB/OU=CA/CN=SynapseDB-CA"
    echo "Generated CA certificate"
fi

# Generate server private key
openssl genrsa -out server.key 2048

# Generate server certificate signing request
openssl req -new -key server.key -out server.csr -subj "/C=US/ST=CA/L=SF/O=SynapseDB/OU=Server/CN=$NODE_NAME"

# Create server certificate extensions
cat > server.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = $NODE_NAME
DNS.2 = localhost
DNS.3 = $NODE_NAME.synapsedb.local
IP.1 = 127.0.0.1
EOF

# Generate server certificate
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -extensions v3_req -extfile server.ext

# Generate client private key for cluster communication
openssl genrsa -out client.key 2048

# Generate client certificate signing request
openssl req -new -key client.key -out client.csr -subj "/C=US/ST=CA/L=SF/O=SynapseDB/OU=Client/CN=synapsedb_cluster"

# Create client certificate extensions
cat > client.ext << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth
EOF

# Generate client certificate
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365 -extensions v3_req -extfile client.ext

# Set proper permissions
chown postgres:postgres "$SSL_DIR"/*
chmod 600 "$SSL_DIR"/*.key
chmod 644 "$SSL_DIR"/*.crt

# Clean up temporary files
rm -f server.csr server.ext client.csr client.ext

echo "SSL certificates generated successfully for $NODE_NAME"
echo "Files created:"
echo "  CA certificate: $SSL_DIR/ca.crt"
echo "  Server certificate: $SSL_DIR/server.crt"
echo "  Server private key: $SSL_DIR/server.key"
echo "  Client certificate: $SSL_DIR/client.crt"
echo "  Client private key: $SSL_DIR/client.key"