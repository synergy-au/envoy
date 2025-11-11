#!/bin/bash
# Script to extract decrypted keys from password-protected certificate keys

CERT_DIR="./tls-termination/test_certs"
TEST_CA_PASSPHRASE="testcapassphrase"
TEST_CLIENT_PASSPHRASE="testclientpassphrase"

# Check if cert directory exists
if [ ! -d "$CERT_DIR" ]; then
    echo "Error: Certificate directory $CERT_DIR not found"
    exit 1
fi

cd "$CERT_DIR" || exit 1

echo "Extracting decrypted keys..."

# Extract testca key
if [ -f "testca.key" ]; then
    echo "Decrypting testca.key..."
    openssl rsa -in testca.key -passin pass:$TEST_CA_PASSPHRASE -out testca-decrypted.key
    echo "✓ Created testca-decrypted.key"
else
    echo "⚠ testca.key not found, skipping..."
fi

# Extract testaggregator key
if [ -f "testaggregator.key" ]; then
    echo "Decrypting testaggregator.key..."
    openssl rsa -in testaggregator.key -passin pass:$TEST_CLIENT_PASSPHRASE -out testaggregator-decrypted.key
    echo "✓ Created testaggregator-decrypted.key"
else
    echo "⚠ testaggregator.key not found, skipping..."
fi

# Extract testdevice key
if [ -f "testdevice.key" ]; then
    echo "Decrypting testdevice.key..."
    openssl rsa -in testdevice.key -passin pass:$TEST_CLIENT_PASSPHRASE -out testdevice-decrypted.key
    echo "✓ Created testdevice-decrypted.key"
else
    echo "⚠ testdevice.key not found, skipping..."
fi

echo ""
echo "Done! Decrypted keys are available:"
ls -lh *-decrypted.key 2>/dev/null || echo "No decrypted keys found"
