#!/bin/bash
# ============================================================================
# Surgewave - Linux Uninstaller
# Stops services, removes binaries (preserves data by default)
# ============================================================================
set -euo pipefail

INSTALL_DIR="/opt/surgewave"
DATA_DIR="/var/lib/surgewave"
LOG_DIR="/var/log/surgewave"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }

PURGE=false
if [[ "${1:-}" == "--purge" ]]; then
    PURGE=true
fi

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}[ERROR]${NC} This script must be run as root (sudo)." >&2
    exit 1
fi

echo "============================================"
echo "  Surgewave - Linux Uninstaller"
echo "============================================"
echo

# Stop and disable services
info "Stopping services..."
systemctl stop surgewave-control 2>/dev/null || true
systemctl stop surgewave-broker 2>/dev/null || true
systemctl disable surgewave-control 2>/dev/null || true
systemctl disable surgewave-broker 2>/dev/null || true

# Remove service files
info "Removing systemd service files..."
rm -f /etc/systemd/system/surgewave-broker.service
rm -f /etc/systemd/system/surgewave-control.service
systemctl daemon-reload

# Remove CLI symlink
info "Removing CLI symlink..."
rm -f /usr/local/bin/surgewave

# Remove install directory
info "Removing installation directory ($INSTALL_DIR)..."
rm -rf "$INSTALL_DIR"

# Optionally remove data
if [[ "$PURGE" == true ]]; then
    warn "Purging data directory ($DATA_DIR)..."
    rm -rf "$DATA_DIR"
    warn "Purging log directory ($LOG_DIR)..."
    rm -rf "$LOG_DIR"
    info "Removing 'surgewave' user..."
    userdel surgewave 2>/dev/null || true
    groupdel surgewave 2>/dev/null || true
else
    info "Data preserved at $DATA_DIR (use --purge to remove)"
    info "Logs preserved at $LOG_DIR (use --purge to remove)"
fi

echo
info "Surgewave uninstalled successfully."
