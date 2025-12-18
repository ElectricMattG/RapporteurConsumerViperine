#!/bin/bash
# Generate htpasswd credentials for Kafka Ingress authentication
#
# Usage: ./generate-kafka-auth.sh <username> <password>
#
# This script generates an htpasswd entry for use in the Kafka Ingress authentication.
# The output should be placed in values.yaml under ingress.auth.htpasswd

set -e

# Check if htpasswd is installed
if ! command -v htpasswd &> /dev/null; then
    echo "❌ Error: htpasswd is not installed"
    echo "Install it with:"
    echo "  macOS: brew install httpd"
    echo "  Ubuntu/Debian: sudo apt-get install apache2-utils"
    echo "  RHEL/CentOS: sudo yum install httpd-tools"
    exit 1
fi

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <username> <password>"
    echo ""
    echo "Example:"
    echo "  $0 kafka mySecurePassword123"
    exit 1
fi

USERNAME="$1"
PASSWORD="$2"

echo "🔐 Generating htpasswd entry for user: $USERNAME"
echo ""

# Generate htpasswd entry
HTPASSWD_ENTRY=$(htpasswd -nb "$USERNAME" "$PASSWORD")

echo "✅ Generated htpasswd entry:"
echo "$HTPASSWD_ENTRY"
echo ""
echo "📝 To use this in Helm values.yaml, add:"
echo ""
echo "ingress:"
echo "  auth:"
echo "    htpasswd: \"$HTPASSWD_ENTRY\""
echo ""
echo "💡 Or update the existing entry in helm/kafka-chart/values.yaml"
