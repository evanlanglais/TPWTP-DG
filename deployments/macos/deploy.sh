#!/bin/bash
set -euo pipefail

# TPWTP Collector Deployment Script for macOS
# Usage: ./deploy.sh [install|update|uninstall|start|stop|restart|status|logs]

REPO_DIR="${REPO_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
INSTALL_DIR="/usr/local/bin"
DATA_DIR="/var/lib/tpwtp-collector"
LOG_DIR="/var/log/tpwtp-collector"
CONFIG_DIR="/etc/tpwtp-collector"
PLIST_NAME="com.tpwtp.collector"
PLIST_PATH="/Library/LaunchDaemons/${PLIST_NAME}.plist"
ENV_FILE="${CONFIG_DIR}/tpwtp-collector.env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_root() {
    if [[ $EUID -ne 0 ]]; then
        log_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

check_dependencies() {
    log_info "Checking dependencies..."

    if ! command -v go &> /dev/null; then
        log_error "Go is not installed. Install with: brew install go"
        exit 1
    fi

    log_info "All dependencies found"
}

create_directories() {
    log_info "Creating directories..."
    mkdir -p "${DATA_DIR}"
    mkdir -p "${LOG_DIR}"
    mkdir -p "${CONFIG_DIR}"
    chmod 755 "${DATA_DIR}" "${LOG_DIR}" "${CONFIG_DIR}"
}

build_binary() {
    log_info "Building tpwtp-collector..."
    cd "${REPO_DIR}"

    # Pull latest if this is a git repo
    if [[ -d ".git" ]]; then
        log_info "Pulling latest changes..."
        sudo -u "${SUDO_USER:-$(whoami)}" git pull origin master || true
    fi

    # Build
    CGO_ENABLED=0 go build -o "${INSTALL_DIR}/tpwtp-collector" ./cmd/collector
    chmod +x "${INSTALL_DIR}/tpwtp-collector"
    log_info "Binary installed to ${INSTALL_DIR}/tpwtp-collector"
}

create_wrapper_script() {
    # Create a wrapper that loads the env file
    cat > "${INSTALL_DIR}/tpwtp-collector-wrapper" << 'WRAPPER'
#!/bin/bash
set -a
source /etc/tpwtp-collector/tpwtp-collector.env
set +a
exec /usr/local/bin/tpwtp-collector "$@"
WRAPPER
    chmod +x "${INSTALL_DIR}/tpwtp-collector-wrapper"
}

install_plist() {
    log_info "Installing launchd plist..."

    # Create plist that uses the wrapper script
    cat > "${PLIST_PATH}" << 'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.tpwtp.collector</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/tpwtp-collector-wrapper</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/var/lib/tpwtp-collector</string>

    <key>RunAtLoad</key>
    <true/>

    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>

    <key>ThrottleInterval</key>
    <integer>10</integer>

    <key>StandardOutPath</key>
    <string>/var/log/tpwtp-collector/stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/var/log/tpwtp-collector/stderr.log</string>

    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>4096</integer>
    </dict>
</dict>
</plist>
PLIST

    chmod 644 "${PLIST_PATH}"
}

setup_env_file() {
    if [[ ! -f "${ENV_FILE}" ]]; then
        log_warn "Environment file not found at ${ENV_FILE}"
        log_info "Copying example env file..."
        cp "${REPO_DIR}/deployments/macos/tpwtp-collector.env.example" "${ENV_FILE}"
        chmod 600 "${ENV_FILE}"
        log_warn "Please edit ${ENV_FILE} with your configuration"
        log_warn "Then run: sudo ./deploy.sh start"
        return 1
    fi
    return 0
}

start_service() {
    log_info "Starting tpwtp-collector service..."
    launchctl load "${PLIST_PATH}" 2>/dev/null || true
    launchctl start "${PLIST_NAME}" 2>/dev/null || true
    sleep 2
    show_status
}

stop_service() {
    log_info "Stopping tpwtp-collector service..."
    launchctl stop "${PLIST_NAME}" 2>/dev/null || true
    launchctl unload "${PLIST_PATH}" 2>/dev/null || true
}

show_status() {
    echo ""
    log_info "Service status:"
    if launchctl list | grep -q "${PLIST_NAME}"; then
        launchctl list "${PLIST_NAME}"
        echo ""
        # Check if process is running
        if pgrep -f "tpwtp-collector" > /dev/null; then
            log_info "Process is running (PID: $(pgrep -f tpwtp-collector-wrapper || pgrep -f tpwtp-collector))"
        else
            log_warn "Service is loaded but process is not running. Check logs."
        fi
    else
        log_warn "Service is not loaded"
    fi
}

show_logs() {
    log_info "Recent logs (stdout):"
    echo "---"
    tail -50 "${LOG_DIR}/stdout.log" 2>/dev/null || echo "No stdout logs yet"
    echo ""
    log_info "Recent logs (stderr):"
    echo "---"
    tail -50 "${LOG_DIR}/stderr.log" 2>/dev/null || echo "No stderr logs yet"
}

do_install() {
    check_root
    check_dependencies
    create_directories
    build_binary
    create_wrapper_script
    install_plist

    if setup_env_file; then
        start_service
    fi

    echo ""
    log_info "Installation complete!"
    log_info "Logs: ${LOG_DIR}/"
    log_info "Config: ${ENV_FILE}"
}

do_update() {
    check_root
    log_info "Updating tpwtp-collector..."
    stop_service
    build_binary
    start_service
    log_info "Update complete!"
}

do_uninstall() {
    check_root
    log_info "Uninstalling tpwtp-collector..."
    stop_service
    rm -f "${PLIST_PATH}"
    rm -f "${INSTALL_DIR}/tpwtp-collector"
    rm -f "${INSTALL_DIR}/tpwtp-collector-wrapper"
    log_info "Uninstalled. Data and config preserved in ${DATA_DIR} and ${CONFIG_DIR}"
}

case "${1:-}" in
    install)
        do_install
        ;;
    update)
        do_update
        ;;
    uninstall)
        do_uninstall
        ;;
    start)
        check_root
        start_service
        ;;
    stop)
        check_root
        stop_service
        ;;
    restart)
        check_root
        stop_service
        start_service
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    *)
        echo "Usage: $0 {install|update|uninstall|start|stop|restart|status|logs}"
        echo ""
        echo "Commands:"
        echo "  install   - First-time installation (builds, installs, configures)"
        echo "  update    - Pull latest code, rebuild, and restart"
        echo "  uninstall - Stop service and remove binaries (keeps data/config)"
        echo "  start     - Start the service"
        echo "  stop      - Stop the service"
        echo "  restart   - Restart the service"
        echo "  status    - Show service status"
        echo "  logs      - Show recent logs"
        exit 1
        ;;
esac
