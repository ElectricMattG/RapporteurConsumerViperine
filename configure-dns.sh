#!/bin/bash
# DNS Configuration Helper for Kafka External Access
#
# This script provides DNS configuration instructions for kafka.rappoteur.ai

set -e

NAMESPACE="kafka"
DOMAIN="kafka.rappoteur.ai"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Kafka DNS Configuration${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Get external IP
echo -e "${YELLOW}🔍 Fetching LoadBalancer external IP...${NC}"
EXTERNAL_IP=$(kubectl get svc kafka-external -n "$NAMESPACE" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)

if [ -z "$EXTERNAL_IP" ]; then
    echo -e "${RED}❌ Failed to get external IP${NC}"
    echo -e "${YELLOW}   Check that the service exists:${NC}"
    echo -e "${YELLOW}   kubectl get svc kafka-external -n $NAMESPACE${NC}"
    exit 1
fi

echo -e "${GREEN}✅ External IP: $EXTERNAL_IP${NC}"
echo ""

# Check current DNS
echo -e "${YELLOW}🔍 Checking current DNS resolution...${NC}"
CURRENT_IP=$(nslookup "$DOMAIN" 2>/dev/null | grep -A1 "Name:" | grep "Address:" | awk '{print $2}' | head -1)

if [ -z "$CURRENT_IP" ]; then
    echo -e "${YELLOW}⚠️  DNS not configured - domain does not resolve${NC}"
else
    if [ "$CURRENT_IP" == "$EXTERNAL_IP" ]; then
        echo -e "${GREEN}✅ DNS correctly configured: $DOMAIN → $CURRENT_IP${NC}"
    else
        echo -e "${RED}⚠️  DNS misconfigured: $DOMAIN → $CURRENT_IP (expected: $EXTERNAL_IP)${NC}"
    fi
fi
echo ""

# DNS Configuration Instructions
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   DNS Configuration Instructions${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}Configure the following A record in your DNS provider:${NC}"
echo ""
echo -e "  ${GREEN}Type:${NC}     A"
echo -e "  ${GREEN}Name:${NC}     kafka.rappoteur.ai"
echo -e "  ${GREEN}Value:${NC}    ${EXTERNAL_IP}"
echo -e "  ${GREEN}TTL:${NC}      300 (or your provider's minimum)"
echo ""

# Provider-specific instructions
echo -e "${YELLOW}📝 Common DNS Provider Instructions:${NC}"
echo ""
echo -e "${BLUE}Cloudflare:${NC}"
echo "  1. Log in to Cloudflare dashboard"
echo "  2. Select your domain (rappoteur.ai)"
echo "  3. Go to DNS → Records"
echo "  4. Click 'Add record'"
echo "  5. Type: A, Name: kafka, IPv4: $EXTERNAL_IP"
echo "  6. Proxy status: DNS only (grey cloud)"
echo "  7. Click 'Save'"
echo ""

echo -e "${BLUE}Route53 (AWS):${NC}"
echo "  aws route53 change-resource-record-sets \\"
echo "    --hosted-zone-id <YOUR_ZONE_ID> \\"
echo "    --change-batch '{"
echo "      \"Changes\": [{"
echo "        \"Action\": \"UPSERT\","
echo "        \"ResourceRecordSet\": {"
echo "          \"Name\": \"$DOMAIN\","
echo "          \"Type\": \"A\","
echo "          \"TTL\": 300,"
echo "          \"ResourceRecords\": [{\"Value\": \"$EXTERNAL_IP\"}]"
echo "        }"
echo "      }]"
echo "    }'"
echo ""

echo -e "${BLUE}Google Cloud DNS:${NC}"
echo "  gcloud dns record-sets create $DOMAIN. \\"
echo "    --zone=<YOUR_ZONE_NAME> \\"
echo "    --type=A \\"
echo "    --ttl=300 \\"
echo "    --rrdatas=$EXTERNAL_IP"
echo ""

echo -e "${BLUE}Azure DNS:${NC}"
echo "  az network dns record-set a add-record \\"
echo "    --resource-group <YOUR_RG> \\"
echo "    --zone-name rappoteur.ai \\"
echo "    --record-set-name kafka \\"
echo "    --ipv4-address $EXTERNAL_IP"
echo ""

# Verification
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Verification${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}After configuring DNS (may take 5-60 minutes to propagate):${NC}"
echo ""
echo -e "1. ${GREEN}Check DNS propagation:${NC}"
echo "   nslookup $DOMAIN"
echo "   dig $DOMAIN +short"
echo ""
echo -e "2. ${GREEN}Check TLS certificate:${NC}"
echo "   kubectl get certificate kafka-tls -n $NAMESPACE"
echo ""
echo -e "3. ${GREEN}Test HTTPS access:${NC}"
echo "   curl -u kafka:kafka123 https://$DOMAIN"
echo ""
echo -e "4. ${GREEN}Test Kafka connectivity:${NC}"
echo "   ./send_kafka_message_external.py list-topics"
echo ""

# Test without DNS
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Testing Without DNS (Direct IP)${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}You can test Kafka connectivity using the IP directly:${NC}"
echo ""
echo -e "  ${GREEN}Bootstrap servers:${NC} $EXTERNAL_IP:9094"
echo ""
echo -e "${YELLOW}Test with kafka-console-producer:${NC}"
echo "  kafka-console-producer \\"
echo "    --bootstrap-server $EXTERNAL_IP:9094 \\"
echo "    --topic test"
echo ""

# Summary
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Summary${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${GREEN}Domain:${NC}           $DOMAIN"
echo -e "  ${GREEN}External IP:${NC}      $EXTERNAL_IP"
echo -e "  ${GREEN}Kafka Port:${NC}       9094"
echo -e "  ${GREEN}Auth:${NC}             kafka / kafka123"
echo -e "  ${GREEN}TLS:${NC}              Enabled (Let's Encrypt)"
echo ""
echo -e "${GREEN}✅ Configure DNS, then wait for certificate to be ready!${NC}"
echo ""
