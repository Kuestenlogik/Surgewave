#!/bin/bash
# ============================================================================
# Surgewave - Linux Installer
# Creates user, directories, copies binaries, installs systemd services
# ============================================================================
set -euo pipefail

INSTALL_DIR="/opt/surgewave"
DATA_DIR="/var/lib/surgewave"
LOG_DIR="/var/log/surgewave"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# Check root
if [[ $EUID -ne 0 ]]; then
    error "This script must be run as root (sudo)."
    exit 1
fi

echo "============================================"
echo "  Surgewave - Linux Installer"
echo "============================================"
echo

# Create surgewave user
info "Creating 'surgewave' system user..."
if id "surgewave" &>/dev/null; then
    info "User 'surgewave' already exists, skipping."
else
    useradd -r -s /bin/false -d "$INSTALL_DIR" surgewave
    info "User 'surgewave' created."
fi

# Create directories
info "Creating directory structure..."
mkdir -p "$INSTALL_DIR"/{broker,cli,control,plugins,models,wasm-plugins}
mkdir -p "$DATA_DIR"/data
mkdir -p "$LOG_DIR"

# Copy binaries
info "Copying broker binaries..."
if [[ -d "$SCRIPT_DIR/broker" ]]; then
    cp -r "$SCRIPT_DIR/broker/"* "$INSTALL_DIR/broker/"
    chmod +x "$INSTALL_DIR/broker/surgewave-broker"
    info "Broker installed to $INSTALL_DIR/broker/"
else
    warn "No broker/ directory found in $SCRIPT_DIR. Skipping broker copy."
fi

info "Copying CLI binaries..."
if [[ -d "$SCRIPT_DIR/cli" ]]; then
    cp -r "$SCRIPT_DIR/cli/"* "$INSTALL_DIR/cli/"
    chmod +x "$INSTALL_DIR/cli/surgewave"
    info "CLI installed to $INSTALL_DIR/cli/"
else
    warn "No cli/ directory found in $SCRIPT_DIR. Skipping CLI copy."
fi

info "Copying Control UI binaries..."
if [[ -d "$SCRIPT_DIR/control" ]]; then
    cp -r "$SCRIPT_DIR/control/"* "$INSTALL_DIR/control/"
    chmod +x "$INSTALL_DIR/control/surgewave-control"
    info "Control UI installed to $INSTALL_DIR/control/"
else
    warn "No control/ directory found in $SCRIPT_DIR. Skipping Control UI copy."
fi

# Set permissions
info "Setting permissions..."
chown -R surgewave:surgewave "$INSTALL_DIR" "$DATA_DIR" "$LOG_DIR"

# Install systemd services
info "Installing systemd services..."
cp "$SCRIPT_DIR/surgewave-broker.service" /etc/systemd/system/
cp "$SCRIPT_DIR/surgewave-control.service" /etc/systemd/system/
systemctl daemon-reload

# Enable and start broker
info "Enabling and starting Surgewave Broker service..."
systemctl enable surgewave-broker
systemctl start surgewave-broker

# Create CLI symlink
if [[ -f "$INSTALL_DIR/cli/surgewave" ]]; then
    ln -sf "$INSTALL_DIR/cli/surgewave" /usr/local/bin/surgewave
    info "CLI symlinked to /usr/local/bin/surgewave"
fi

echo
echo "============================================"
echo "  Surgewave installed successfully!"
echo "============================================"
echo
echo "  Broker:     systemctl status surgewave-broker"
echo "  Control UI: systemctl enable --now surgewave-control"
echo "  CLI:        surgewave --help"
echo "  Dashboard:  http://localhost:5050"
echo
echo "  Directories:"
echo "    Install:  $INSTALL_DIR"
echo "    Data:     $DATA_DIR"
echo "    Logs:     $LOG_DIR"
echo "    Plugins:  $INSTALL_DIR/plugins"
echo
