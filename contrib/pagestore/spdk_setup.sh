#!/usr/bin/env bash
#
# S0 bring-up: bind the dedicated control disk to SPDK and benchmark it.
#
# This realizes step S0 of SPDK_NOTES.md.  It reserves hugepages, binds the
# control NVMe to vfio-pci (userspace), and runs a raw throughput baseline.
#
# SAFETY: PCI_ALLOWED restricts SPDK's setup.sh to the control disk ONLY, so the
# system disk and the /home NVMe are never rebound.  Always keep CTRL_PCI correct
# for the box you run on.
#
# Usage:
#   contrib/pagestore/spdk_setup.sh            # reserve hugepages + bind + bench
#   contrib/pagestore/spdk_setup.sh status     # show current binding
#   contrib/pagestore/spdk_setup.sh reset      # give the disk back to the kernel
#
set -euo pipefail

SPDK_DIR="${SPDK_DIR:-$HOME/spdk}"
CTRL_PCI="${CTRL_PCI:-0000:06:00.0}"   # WD SN850 465.8G -- the bare control disk
HUGEMEM="${HUGEMEM:-4096}"             # MiB of hugepages to reserve

mode="${1:-up}"

if [ ! -x "$SPDK_DIR/scripts/setup.sh" ]; then
	echo "SPDK not found at $SPDK_DIR (set SPDK_DIR). Build it first:"
	echo "  git clone --branch v26.01 https://github.com/spdk/spdk ~/spdk"
	echo "  cd ~/spdk && git submodule update --init && ./configure && make -j"
	exit 1
fi

case "$mode" in
status)
	sudo PCI_ALLOWED="$CTRL_PCI" "$SPDK_DIR/scripts/setup.sh" status
	exit 0
	;;
reset)
	# hand the control disk back to the kernel nvme driver
	sudo PCI_ALLOWED="$CTRL_PCI" "$SPDK_DIR/scripts/setup.sh" reset
	exit 0
	;;
up) ;;
*)
	echo "usage: $0 [up|status|reset]"; exit 2
	;;
esac

# 0. IOMMU must be active for safe vfio-pci DMA (else SPDK uses unsafe uio).
if [ -z "$(ls -A /sys/kernel/iommu_groups 2>/dev/null)" ]; then
	echo "WARNING: IOMMU is not active (empty /sys/kernel/iommu_groups)."
	echo "  Add 'intel_iommu=on iommu=pt' to the kernel cmdline and reboot for"
	echo "  safe DMA; otherwise SPDK falls back to unsafe uio/no-IOMMU mode."
	echo
fi

# 1. reserve hugepages + bind ONLY the control disk to vfio-pci.
echo "=== setup: hugepages=${HUGEMEM}MiB, bind $CTRL_PCI to vfio-pci ==="
sudo HUGEMEM="$HUGEMEM" PCI_ALLOWED="$CTRL_PCI" "$SPDK_DIR/scripts/setup.sh"

# 2. confirm.
echo "=== status ==="
sudo PCI_ALLOWED="$CTRL_PCI" "$SPDK_DIR/scripts/setup.sh" status
grep -i hugepages_total /proc/meminfo

# 3. validate the disk under SPDK: enumerate then a 4 KiB randread baseline.
#    (In SPDK v26.01 these tools install as build/bin/spdk_nvme_{identify,perf}.)
echo "=== identify ==="
sudo "$SPDK_DIR/build/bin/spdk_nvme_identify" -r "trtype:PCIe traddr:$CTRL_PCI" 2>&1 | head -40
echo "=== perf: 4 KiB randread, qd=128, 20s (raw ceiling) ==="
sudo "$SPDK_DIR/build/bin/spdk_nvme_perf" -q 128 -o 4096 -w randread -t 20 \
	-r "trtype:PCIe traddr:$CTRL_PCI"

echo
echo "S0 done. To return the disk to the kernel: $0 reset"
