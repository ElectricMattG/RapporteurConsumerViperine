#!/bin/bash

# Service Management Script for RAG Frontend
# Manages RAG service and Frontend service via Docker Compose

set -e

COMPOSE_FILE="docker-compose.services.yml"
PROJECT_NAME="rag-frontend"

show_help() {
    cat << EOF
RAG Frontend Service Management

Usage: $0 [COMMAND]

Commands:
    start       Build and start all services
    stop        Stop all services
    restart     Restart all services
    logs        Show logs from all services
    logs-rag    Show logs from RAG service only
    logs-ui     Show logs from Frontend service only
    status      Show service status
    build       Rebuild service images
    clean       Stop and remove all containers, networks, and volumes
    shell-rag   Open shell in RAG service container
    shell-ui    Open shell in Frontend service container
    help        Show this help message

Examples:
    $0 start                # Start both services
    $0 logs -f              # Follow logs from both services
    $0 restart              # Restart all services
    $0 clean                # Clean up everything

Environment Variables:
    EMBEDDING_SERVICE_URL   - Embedding service endpoint (default: https://embeddings.rapporteur.ai)
    GCP_PROJECT_ID          - Google Cloud project ID for Gemini
    GCP_LOCATION            - GCP region (default: us-central1)

EOF
}

start_services() {
    echo "🚀 Starting RAG Frontend services..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up -d
    echo ""
    echo "✅ Services started!"
    echo "   • RAG Service:      http://localhost:8000"
    echo "   • Frontend UI:      http://localhost:5000"
    echo ""
    echo "📋 Check status: $0 status"
    echo "📋 View logs:    $0 logs -f"
}

stop_services() {
    echo "🛑 Stopping services..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down
    echo "✅ Services stopped"
}

restart_services() {
    echo "🔄 Restarting services..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" restart
    echo "✅ Services restarted"
}

show_logs() {
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs "$@"
}

show_rag_logs() {
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs "$@" rag-service
}

show_ui_logs() {
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs "$@" frontend-service
}

show_status() {
    echo "📊 Service Status:"
    echo ""
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" ps
}

build_images() {
    echo "🔨 Building service images..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" build "$@"
    echo "✅ Build complete"
}

clean_all() {
    echo "🧹 Cleaning up all services..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down -v
    echo "✅ Cleanup complete"
}

shell_rag() {
    echo "🐚 Opening shell in RAG service..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" exec rag-service /bin/bash
}

shell_ui() {
    echo "🐚 Opening shell in Frontend service..."
    docker-compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" exec frontend-service /bin/bash
}

# Main command dispatcher
case "${1:-help}" in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    logs)
        shift
        show_logs "$@"
        ;;
    logs-rag)
        shift
        show_rag_logs "$@"
        ;;
    logs-ui)
        shift
        show_ui_logs "$@"
        ;;
    status)
        show_status
        ;;
    build)
        shift
        build_images "$@"
        ;;
    clean)
        clean_all
        ;;
    shell-rag)
        shell_rag
        ;;
    shell-ui)
        shell_ui
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
