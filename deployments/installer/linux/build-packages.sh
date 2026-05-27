#!/bin/bash
# Build .deb and .rpm packages for Surgewave
# Usage: ./build-packages.sh [version] [arch]
#   version: e.g., 0.1.0 (default: 0.1.0)
#   arch: amd64 or arm64 (default: amd64)

set -e

VERSION="${1:-0.1.0}"
ARCH="${2:-amd64}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALLER_DIR="$(dirname "$SCRIPT_DIR")"
# REPO_ROOT walks two levels up — the installer lives at
# deployments/installer/linux/, so dirname(dirname(installer-dir)) lands
# on the repo root (where src/ and Kuestenlogik.Surgewave.slnx sit).
REPO_ROOT="$(dirname "$(dirname "$INSTALLER_DIR")")"
BUILD_DIR="$SCRIPT_DIR/build"
RID="linux-x64"
[ "$ARCH" = "arm64" ] && RID="linux-arm64"

echo "Building Surgewave $VERSION ($ARCH) packages..."

# Clean
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# Publish binaries
echo "Publishing broker..."
dotnet publish "$REPO_ROOT/src/Kuestenlogik.Surgewave.Broker" -c Release -r $RID --self-contained -o "$BUILD_DIR/broker" -p:Version=$VERSION

echo "Publishing CLI..."
dotnet publish "$REPO_ROOT/src/Kuestenlogik.Surgewave.Tool" -c Release -r $RID --self-contained -o "$BUILD_DIR/cli" -p:Version=$VERSION

echo "Publishing Control..."
dotnet publish "$REPO_ROOT/src/Kuestenlogik.Surgewave.Control" -c Release -r $RID --self-contained -o "$BUILD_DIR/control" -p:Version=$VERSION

# ============================================================
# Build .deb package
# ============================================================
echo "Building .deb package..."

DEB_DIR="$BUILD_DIR/deb-root"
mkdir -p "$DEB_DIR/DEBIAN"
mkdir -p "$DEB_DIR/opt/surgewave/broker"
mkdir -p "$DEB_DIR/opt/surgewave/cli"
mkdir -p "$DEB_DIR/opt/surgewave/control"
mkdir -p "$DEB_DIR/opt/surgewave/plugins"
mkdir -p "$DEB_DIR/opt/surgewave/models"
mkdir -p "$DEB_DIR/opt/surgewave/wasm-plugins"
mkdir -p "$DEB_DIR/etc/surgewave"
mkdir -p "$DEB_DIR/etc/systemd/system"
mkdir -p "$DEB_DIR/usr/bin"
mkdir -p "$DEB_DIR/var/lib/surgewave/data"
mkdir -p "$DEB_DIR/var/log/surgewave"

# Copy binaries
cp -r "$BUILD_DIR/broker/"* "$DEB_DIR/opt/surgewave/broker/"
cp -r "$BUILD_DIR/cli/"* "$DEB_DIR/opt/surgewave/cli/"
cp -r "$BUILD_DIR/control/"* "$DEB_DIR/opt/surgewave/control/"

# Config
cp "$INSTALLER_DIR/config/appsettings.json" "$DEB_DIR/etc/surgewave/"
cp "$INSTALLER_DIR/config/appsettings.Production.json" "$DEB_DIR/etc/surgewave/"

# Systemd services
cp "$SCRIPT_DIR/surgewave-broker.service" "$DEB_DIR/etc/systemd/system/"
cp "$SCRIPT_DIR/surgewave-control.service" "$DEB_DIR/etc/systemd/system/"

# CLI symlink
ln -sf /opt/surgewave/cli/surgewave "$DEB_DIR/usr/bin/surgewave"

# DEBIAN control files
cp "$SCRIPT_DIR/deb/DEBIAN/control" "$DEB_DIR/DEBIAN/"
cp "$SCRIPT_DIR/deb/DEBIAN/postinst" "$DEB_DIR/DEBIAN/"
cp "$SCRIPT_DIR/deb/DEBIAN/prerm" "$DEB_DIR/DEBIAN/"
cp "$SCRIPT_DIR/deb/DEBIAN/postrm" "$DEB_DIR/DEBIAN/"

# Update version and architecture in control file
sed -i "s/Version: .*/Version: $VERSION/" "$DEB_DIR/DEBIAN/control"
sed -i "s/Architecture: .*/Architecture: $ARCH/" "$DEB_DIR/DEBIAN/control"

# Set permissions
chmod 755 "$DEB_DIR/DEBIAN/postinst"
chmod 755 "$DEB_DIR/DEBIAN/prerm"
chmod 755 "$DEB_DIR/DEBIAN/postrm"
chmod 755 "$DEB_DIR/opt/surgewave/broker/surgewave-broker"
chmod 755 "$DEB_DIR/opt/surgewave/control/surgewave-control" 2>/dev/null || true
chmod 755 "$DEB_DIR/opt/surgewave/cli/surgewave" 2>/dev/null || true

# Build .deb
DEB_FILE="$BUILD_DIR/surgewave_${VERSION}_${ARCH}.deb"
dpkg-deb --build "$DEB_DIR" "$DEB_FILE" 2>/dev/null && echo "Created: $DEB_FILE" || echo "SKIP: dpkg-deb not available (install dpkg on this system)"

# ============================================================
# Build .rpm package
# ============================================================
echo "Building .rpm package..."

RPM_DIR="$BUILD_DIR/rpm-build"
mkdir -p "$RPM_DIR/"{SOURCES,SPECS,BUILD,RPMS,SRPMS}

# Copy sources
cp -r "$BUILD_DIR/broker" "$RPM_DIR/SOURCES/"
cp -r "$BUILD_DIR/cli" "$RPM_DIR/SOURCES/"
cp -r "$BUILD_DIR/control" "$RPM_DIR/SOURCES/"
cp "$INSTALLER_DIR/config/appsettings.json" "$RPM_DIR/SOURCES/"
cp "$INSTALLER_DIR/config/appsettings.Production.json" "$RPM_DIR/SOURCES/"
cp "$SCRIPT_DIR/surgewave-broker.service" "$RPM_DIR/SOURCES/"
cp "$SCRIPT_DIR/surgewave-control.service" "$RPM_DIR/SOURCES/"

# Update spec version
sed "s/Version:.*/Version:        $VERSION/" "$SCRIPT_DIR/rpm/surgewave.spec" > "$RPM_DIR/SPECS/surgewave.spec"

# Build RPM
RPM_FILE="$BUILD_DIR/surgewave-${VERSION}.${ARCH/amd64/x86_64}.rpm"
rpmbuild --define "_topdir $RPM_DIR" -bb "$RPM_DIR/SPECS/surgewave.spec" 2>/dev/null && \
    cp "$RPM_DIR/RPMS/"*/*.rpm "$RPM_FILE" && echo "Created: $RPM_FILE" || \
    echo "SKIP: rpmbuild not available (install rpm-build on this system)"

echo ""
echo "Done. Packages in: $BUILD_DIR/"
ls -lh "$BUILD_DIR/"*.deb "$BUILD_DIR/"*.rpm 2>/dev/null || echo "(Some packages may have been skipped due to missing tools)"
