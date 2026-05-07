Name:           storm
Version:        0.1.0
Release:        1%{?dist}
Summary:        Storm - High-performance event streaming platform
License:        Apache-2.0
URL:            https://github.com/Kuestenlogik/Storm
BuildArch:      x86_64

%description
A drop-in Kafka replacement built with .NET 10.
Includes broker, CLI, and Control UI.
Features: Kafka 4.0 protocol, native high-performance protocol,
visual pipeline editor, AI integration, schema registry,
113 connectors, 10 storage engines.

%install
mkdir -p %{buildroot}/opt/storm/broker
mkdir -p %{buildroot}/opt/storm/cli
mkdir -p %{buildroot}/opt/storm/control
mkdir -p %{buildroot}/opt/storm/plugins
mkdir -p %{buildroot}/opt/storm/models
mkdir -p %{buildroot}/opt/storm/wasm-plugins
mkdir -p %{buildroot}/etc/storm
mkdir -p %{buildroot}/var/lib/storm/data
mkdir -p %{buildroot}/var/log/storm
mkdir -p %{buildroot}/usr/lib/systemd/system
mkdir -p %{buildroot}/usr/bin

# Copy binaries (from SOURCES)
cp -r %{_sourcedir}/broker/* %{buildroot}/opt/storm/broker/
cp -r %{_sourcedir}/cli/* %{buildroot}/opt/storm/cli/ 2>/dev/null || true
cp -r %{_sourcedir}/control/* %{buildroot}/opt/storm/control/ 2>/dev/null || true

# Config
cp %{_sourcedir}/appsettings.json %{buildroot}/etc/storm/
cp %{_sourcedir}/appsettings.Production.json %{buildroot}/etc/storm/

# Systemd units
cp %{_sourcedir}/storm-broker.service %{buildroot}/usr/lib/systemd/system/
cp %{_sourcedir}/storm-control.service %{buildroot}/usr/lib/systemd/system/

# CLI symlink
ln -sf /opt/storm/cli/storm %{buildroot}/usr/bin/storm

%files
/opt/storm/
/etc/storm/
/usr/lib/systemd/system/storm-broker.service
/usr/lib/systemd/system/storm-control.service
/usr/bin/storm
%dir /var/lib/storm
%dir /var/log/storm

%pre
getent group storm >/dev/null || groupadd -r storm
getent passwd storm >/dev/null || useradd -r -g storm -d /var/lib/storm -s /sbin/nologin storm

%post
chown -R storm:storm /var/lib/storm /var/log/storm /opt/storm
chmod +x /opt/storm/broker/Kuestenlogik.Storm.Broker
systemctl daemon-reload
systemctl enable storm-broker
echo "Storm installed. Start with: sudo systemctl start storm-broker"

%preun
systemctl stop storm-broker 2>/dev/null || true
systemctl stop storm-control 2>/dev/null || true
systemctl disable storm-broker 2>/dev/null || true
systemctl disable storm-control 2>/dev/null || true

%postun
systemctl daemon-reload
