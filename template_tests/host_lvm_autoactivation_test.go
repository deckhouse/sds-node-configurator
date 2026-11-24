/*
Copyright 2026 Flant JSC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package templatetests

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

const (
	autoactivationTemplate = "../templates/agent/nodegroupconfiguration-host-lvm-autoactivation.yaml"
	autoactivationEntry    = "host_lvm_configure_autoactivation"
	clusterUUIDValue       = `{{ ((.Values.global).discovery).clusterUUID | default "" | squote }}`
	testClusterUUID        = "0b9c6d3e-4a59-4f6e-9a39-2a1f5ad1c0de"
	moduleTag              = "@storage.deckhouse.io/cluster=" + testClusterUUID
	legacyTag              = "@storage.deckhouse.io/enabled=true"
	otherClusterTag        = "@storage.deckhouse.io/cluster=11111111-2222-3333-4444-555555555555"
)

const lvmconfigStub = `#!/bin/bash
echo "$*" >> "$STUB_DIR/lvmconfig.calls"
case $1 in
  activation/auto_activation_volume_list)
    if [ -s "$STUB_DIR/list" ]; then
      cat "$STUB_DIR/list"
      exit 0
    fi
    echo "  Configuration node activation/auto_activation_volume_list not found" >&2
    exit 5
    ;;
  --config)
    value=${2#activation/auto_activation_volume_list=}
    echo "auto_activation_volume_list=$value" > "$STUB_DIR/list"
    printf 'activation {\n\tauto_activation_volume_list=%s\n}\n' "$value"
    ;;
  *)
    exit 1
    ;;
esac
`

const findfsStub = `#!/bin/bash
while read -r spec dev; do
  if [ "$spec" = "$1" ]; then
    echo "$dev"
    exit 0
  fi
done < "$STUB_DIR/findfs.map"
echo "findfs: unable to resolve '$1'" >&2
exit 1
`

func ngcScript(t *testing.T, path, entry string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var body []string
	inContent := false
	for _, line := range strings.Split(string(raw), "\n") {
		if !inContent {
			inContent = line == "  content: |"
			continue
		}
		if line != "" && !strings.HasPrefix(line, "    ") {
			t.Fatalf("%s: a line of spec.content is not indented by four spaces: %q", path, line)
		}
		body = append(body, strings.TrimPrefix(line, "    "))
	}
	if !inContent {
		t.Fatalf("%s has no spec.content block", path)
	}
	for len(body) > 0 && body[len(body)-1] == "" {
		body = body[:len(body)-1]
	}
	if len(body) == 0 || body[len(body)-1] != entry {
		t.Fatalf("%s: spec.content must end with a call of %s", path, entry)
	}
	return strings.Join(body[:len(body)-1], "\n") + "\n"
}

func renderClusterUUID(t *testing.T, script, clusterUUID string) string {
	t.Helper()
	if strings.Count(script, clusterUUIDValue) != 1 {
		t.Fatalf("the script must take the cluster UUID from %s exactly once", clusterUUIDValue)
	}
	script = strings.Replace(script, clusterUUIDValue, "'"+strings.ReplaceAll(clusterUUID, "'", `'\''`)+"'", 1)
	if strings.Contains(script, "{{") {
		t.Fatal("the script has a template expression the test does not render")
	}
	return script
}

type fakeNode struct {
	t       *testing.T
	root    string
	stubs   string
	mounts  []string
	swaps   []string
	fstab   []string
	cmdline string
	resume  string
	findfs  map[string]string
	nextID  int
	realLVM bool
	cluster string
}

func newFakeNode(t *testing.T) *fakeNode {
	t.Helper()
	n := &fakeNode{t: t, root: t.TempDir(), findfs: map[string]string{}, cmdline: "BOOT_IMAGE=/vmlinuz ro quiet", nextID: 20, cluster: testClusterUUID}
	n.stubs = filepath.Join(n.root, "stubs")
	for _, dir := range []string{"proc/self", "sys/dev/block", "sys/class/block", "dev/mapper", "etc/lvm", "stubs"} {
		n.mkdir(dir)
	}
	n.write("etc/lvm/lvm.conf", "devices {\n\tglobal_filter=[\"r|^/dev/loop[0-9]+|\"]\n}\n")
	n.writeExec("stubs/lvmconfig", lvmconfigStub)
	n.writeExec("stubs/findfs", findfsStub)
	return n
}

func (n *fakeNode) path(rel string) string { return filepath.Join(n.root, rel) }

func (n *fakeNode) mkdir(rel string) {
	n.t.Helper()
	if err := os.MkdirAll(n.path(rel), 0o755); err != nil {
		n.t.Fatal(err)
	}
}

func (n *fakeNode) write(rel, content string) {
	n.t.Helper()
	n.mkdir(filepath.Dir(rel))
	if err := os.WriteFile(n.path(rel), []byte(content), 0o644); err != nil {
		n.t.Fatal(err)
	}
}

func (n *fakeNode) writeExec(rel, content string) {
	n.t.Helper()
	n.write(rel, content)
	if err := os.Chmod(n.path(rel), 0o755); err != nil {
		n.t.Fatal(err)
	}
}

func (n *fakeNode) symlink(target, rel string) {
	n.t.Helper()
	n.mkdir(filepath.Dir(rel))
	if err := os.Symlink(target, n.path(rel)); err != nil {
		n.t.Fatal(err)
	}
}

func (n *fakeNode) register(name, majmin, dir string) {
	n.t.Helper()
	n.symlink(n.path(dir), "sys/class/block/"+name)
	if majmin != "" {
		n.symlink(n.path(dir), "sys/dev/block/"+majmin)
	}
	n.write("dev/"+name, "")
}

func (n *fakeNode) disk(name, majmin string) {
	n.t.Helper()
	dir := "sys/devices/pci0000:00/block/" + name
	n.mkdir(dir)
	n.register(name, majmin, dir)
}

func (n *fakeNode) partition(disk, name, majmin string) {
	n.t.Helper()
	dir := "sys/devices/pci0000:00/block/" + disk + "/" + name
	n.write(dir+"/partition", "1\n")
	n.register(name, majmin, dir)
}

func (n *fakeNode) dm(name, majmin, dmName, uuid string, slaves ...string) {
	n.t.Helper()
	dir := "sys/devices/virtual/block/" + name
	n.write(dir+"/dm/name", dmName+"\n")
	n.write(dir+"/dm/uuid", uuid+"\n")
	n.mkdir(dir + "/slaves")
	for _, s := range slaves {
		target, err := os.Readlink(n.path("sys/class/block/" + s))
		if err != nil {
			n.t.Fatalf("slave %s of %s is not registered: %v", s, name, err)
		}
		n.symlink(target, dir+"/slaves/"+s)
	}
	n.register(name, majmin, dir)
	n.symlink("../"+name, "dev/mapper/"+dmName)
}

func dmEscape(s string) string { return strings.ReplaceAll(s, "-", "--") }

func (n *fakeNode) lv(name, majmin, vg, lv string, slaves ...string) {
	n.t.Helper()
	n.dm(name, majmin, dmEscape(vg)+"-"+dmEscape(lv), "LVM-"+strings.Repeat("a", 64)+name, slaves...)
}

func (n *fakeNode) mount(majmin, target, fstype, source string) {
	n.nextID++
	n.mounts = append(n.mounts, fmt.Sprintf("%d 1 %s / %s rw,relatime shared:%d - %s %s rw", n.nextID, majmin, target, n.nextID, fstype, source))
}

func (n *fakeNode) setList(value string) {
	n.write("stubs/list", "auto_activation_volume_list="+value+"\n")
}

func (n *fakeNode) flush() {
	n.t.Helper()
	n.write("proc/self/mountinfo", strings.Join(n.mounts, "\n")+"\n")
	n.write("proc/swaps", "Filename\t\t\t\tType\t\tSize\t\tUsed\t\tPriority\n"+strings.Join(n.swaps, "\n")+"\n")
	n.write("proc/cmdline", n.cmdline+"\n")
	n.write("etc/fstab", strings.Join(n.fstab, "\n")+"\n")
	if n.resume != "" {
		n.write("etc/initramfs-tools/conf.d/resume", n.resume+"\n")
	}
	var findfs strings.Builder
	for spec, dev := range n.findfs {
		fmt.Fprintf(&findfs, "%s %s\n", spec, dev)
	}
	n.write("stubs/findfs.map", findfs.String())
}

type runResult struct {
	stderr string
	err    error
}

func (n *fakeNode) run() runResult {
	n.t.Helper()
	n.flush()
	lib := filepath.Join(n.root, "ngc.sh")
	if err := os.WriteFile(lib, []byte(renderClusterUUID(n.t, ngcScript(n.t, autoactivationTemplate, autoactivationEntry), n.cluster)), 0o644); err != nil {
		n.t.Fatal(err)
	}
	stubDir := n.stubs
	if n.realLVM {
		stubDir = filepath.Join(n.root, "realstubs")
		if err := os.MkdirAll(stubDir, 0o755); err != nil {
			n.t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(stubDir, "findfs"), []byte(findfsStub), 0o755); err != nil {
			n.t.Fatal(err)
		}
	}
	script := fmt.Sprintf(`source %q
host_lvm_proc=%q
host_lvm_sys=%q
host_lvm_dev=%q
host_lvm_fstab=%q
host_lvm_initramfs_resume=%q
%s`, lib, n.path("proc"), n.path("sys"), n.path("dev"), n.path("etc/fstab"),
		n.path("etc/initramfs-tools/conf.d/resume"), autoactivationEntry)
	cmd := exec.Command("bash", "-eEo", "pipefail", "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+stubDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"STUB_DIR="+n.stubs,
		"LVM_SYSTEM_DIR="+n.path("etc/lvm"),
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	return runResult{stderr: stderr.String(), err: err}
}

var quoted = regexp.MustCompile(`"[^"]*"`)

func listEntries(value string) []string {
	var out []string
	for _, q := range quoted.FindAllString(value, -1) {
		out = append(out, strings.Trim(q, `"`))
	}
	return out
}

func (n *fakeNode) list() ([]string, bool) {
	n.t.Helper()
	raw, err := os.ReadFile(n.path("stubs/list"))
	if os.IsNotExist(err) {
		return nil, false
	}
	if err != nil {
		n.t.Fatal(err)
	}
	return listEntries(string(raw)), true
}

func (n *fakeNode) writes() int {
	raw, err := os.ReadFile(n.path("stubs/lvmconfig.calls"))
	if err != nil {
		return 0
	}
	return strings.Count(string(raw), "--config ")
}

func (n *fakeNode) expectList(r runResult, want ...string) {
	n.t.Helper()
	if r.err != nil {
		n.t.Fatalf("the script failed: %v\n%s", r.err, r.stderr)
	}
	got, ok := n.list()
	if !ok {
		n.t.Fatalf("auto_activation_volume_list was not written\n%s", r.stderr)
	}
	if !slices.Equal(got, want) {
		n.t.Fatalf("auto_activation_volume_list = %q, want %q\n%s", got, want, r.stderr)
	}
}

func (n *fakeNode) rootOnPartition() {
	n.disk("vda", "252:0")
	n.partition("vda", "vda1", "252:1")
	n.mount("252:1", "/", "ext4", "/dev/vda1")
}

func TestVGOfDMName(t *testing.T) {
	lib := renderClusterUUID(t, ngcScript(t, autoactivationTemplate, autoactivationEntry), testClusterUUID)
	for _, tc := range []struct {
		name, want string
		ok         bool
	}{
		{"vg-lv", "vg", true},
		{"ubuntu--vg-ubuntu--lv", "ubuntu-vg", true},
		{"a---b", "a-", true},
		{"vg-pool-tpool", "vg", true},
		{"vg-pool_tdata", "vg", true},
		{"x--y--z-w", "x-y-z", true},
		{"nodash", "", false},
		{"only--doubled", "", false},
		{"-lv", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("bash", "-eEo", "pipefail", "-c", lib+"host_lvm_vg_of_dm_name \"$1\"", "test", tc.name)
			out, err := cmd.Output()
			if tc.ok != (err == nil) {
				t.Fatalf("decoding %q: err=%v, want ok=%v", tc.name, err, tc.ok)
			}
			if got := strings.TrimSpace(string(out)); got != tc.want {
				t.Fatalf("decoding %q gave %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

func TestRootOnPlainPartitionGetsOnlyTheModuleTag(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.expectList(n.run(), moduleTag)
}

func TestRootVolumeGroupIsKept(t *testing.T) {
	n := newFakeNode(t)
	n.disk("sda", "8:0")
	n.partition("sda", "sda3", "8:3")
	n.lv("dm-0", "253:0", "ubuntu-vg", "ubuntu-lv", "sda3")
	n.mount("253:0", "/", "ext4", "/dev/mapper/ubuntu--vg-ubuntu--lv")
	n.expectList(n.run(), "ubuntu-vg", moduleTag)
}

func TestRootOnEncryptedLogicalVolume(t *testing.T) {
	n := newFakeNode(t)
	n.disk("nvme0n1", "259:0")
	n.partition("nvme0n1", "nvme0n1p3", "259:3")
	n.lv("dm-0", "253:0", "system", "root", "nvme0n1p3")
	n.dm("dm-1", "253:1", "luks-root", "CRYPT-LUKS2-0123-luks-root", "dm-0")
	n.mount("253:1", "/", "xfs", "/dev/mapper/luks-root")
	n.expectList(n.run(), "system", moduleTag)
}

func TestRootOnThinVolumeOverRAID(t *testing.T) {
	n := newFakeNode(t)
	n.disk("sda", "8:0")
	n.disk("sdb", "8:16")
	n.dm("md127", "9:127", "md127", "", "sda", "sdb")
	n.lv("dm-2", "253:2", "os", "pool_tmeta", "md127")
	n.lv("dm-3", "253:3", "os", "pool_tdata", "md127")
	n.lv("dm-4", "253:4", "os", "pool-tpool", "dm-2", "dm-3")
	n.lv("dm-5", "253:5", "os", "root", "dm-4")
	n.mount("253:5", "/", "ext4", "/dev/mapper/os-root")
	n.expectList(n.run(), "os", moduleTag)
}

func TestBootCriticalVolumeGroupsOutsideTheRoot(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.disk("vdb", "252:16")
	n.disk("vdc", "252:32")
	n.disk("vdd", "252:48")
	n.lv("dm-0", "253:0", "varvg", "var", "vdb")
	n.lv("dm-1", "253:1", "swapvg", "swap", "vdc")
	n.lv("dm-2", "253:2", "homevg", "home", "vdd")
	n.mount("253:0", "/var", "xfs", "/dev/mapper/varvg-var")
	n.swaps = []string{"/dev/dm-1                               partition\t2097148\t\t0\t\t-2"}
	n.fstab = []string{
		"# /etc/fstab",
		"UUID=root-uuid / ext4 defaults 0 1",
		"/dev/mapper/varvg-var /var xfs defaults 0 0",
		"UUID=home-uuid /home xfs defaults 0 0",
		"/dev/datavg/data /data xfs defaults,nofail 0 0",
		"/dev/mapper/backup--vg-backup /backup xfs noauto 0 0",
		"tmpfs /tmp tmpfs defaults 0 0",
		"/swapfile none swap sw 0 0",
		"server:/export /mnt/nfs nfs defaults 0 0",
	}
	n.findfs["UUID=root-uuid"] = "/dev/vda1"
	n.findfs["UUID=home-uuid"] = "/dev/dm-2"
	n.expectList(n.run(), "backup-vg", "datavg", "homevg", "swapvg", "varvg", moduleTag)
}

func TestKernelCommandLineAndResumeVolumeGroups(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.cmdline = "BOOT_IMAGE=(hd0,gpt2)/vmlinuz root=/dev/mapper/rl-root ro resume=/dev/mapper/rl-swap rd.lvm.lv=rl/root rd.lvm.lv=rl/swap rd.lvm.vg=extra rd.lvm.vg="
	n.resume = `RESUME="UUID=resume-uuid"`
	n.disk("vdb", "252:16")
	n.lv("dm-7", "253:7", "hibernate", "swap", "vdb")
	n.findfs["UUID=resume-uuid"] = "/dev/dm-7"
	n.expectList(n.run(), "extra", "hibernate", "rl", moduleTag)
}

func TestKubeletMountsAreLeftOut(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.disk("vdb", "252:16")
	n.lv("dm-0", "253:0", "e2e-vg", "pvc-1", "vdb")
	n.mount("253:0", "/var/lib/kubelet/pods/uid/volumes/kubernetes.io~csi/pvc-1/mount", "ext4", "/dev/mapper/e2e--vg-pvc--1")
	n.mount("253:0", "/var/lib/kubelet/plugins/kubernetes.io/csi/local.csi.storage.deckhouse.io/abc/globalmount", "ext4", "/dev/mapper/e2e--vg-pvc--1")
	n.expectList(n.run(), moduleTag)
}

func TestEscapedMountPointUnderKubelet(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.disk("vdb", "252:16")
	n.lv("dm-0", "253:0", "e2e-vg", "pvc-1", "vdb")
	n.mount("253:0", `/var/lib/kubelet/pods/uid/volumes/a\040b`, "ext4", "/dev/mapper/e2e--vg-pvc--1")
	n.mount("253:0", `/srv/with\040space`, "ext4", "/dev/mapper/e2e--vg-pvc--1")
	n.expectList(n.run(), "e2e-vg", moduleTag)
}

func TestRootOnBtrfsIsResolvedByItsSource(t *testing.T) {
	n := newFakeNode(t)
	n.disk("vda", "252:0")
	n.partition("vda", "vda2", "252:2")
	n.lv("dm-0", "253:0", "alt", "root", "vda2")
	n.mount("0:33", "/", "btrfs", "/dev/mapper/alt-root")
	n.mount("0:22", "/run", "tmpfs", "tmpfs")
	n.mount("0:40", "/opt/zfs", "zfs", "tank/opt")
	n.expectList(n.run(), "alt", moduleTag)
}

func TestRootWithoutABlockDeviceIsNotAnLVMConcern(t *testing.T) {
	n := newFakeNode(t)
	n.mount("0:30", "/", "overlay", "overlay")
	n.expectList(n.run(), moduleTag)
}

func TestUnresolvableRootLeavesLVMConfAlone(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(n *fakeNode)
	}{
		{"block device missing from sysfs", func(n *fakeNode) {
			n.mount("8:1", "/", "ext4", "/dev/root")
		}},
		{"device path missing on a btrfs root", func(n *fakeNode) {
			n.mount("0:33", "/", "btrfs", "/dev/mapper/gone-root")
		}},
		{"undecodable LVM name", func(n *fakeNode) {
			n.disk("sda", "8:0")
			n.dm("dm-0", "253:0", "weird", "LVM-0123", "sda")
			n.mount("253:0", "/", "ext4", "/dev/mapper/weird")
		}},
		{"no root in the mount table", func(n *fakeNode) {
			n.disk("sda", "8:0")
			n.mount("8:0", "/data", "ext4", "/dev/sda")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newFakeNode(t)
			tc.setup(n)
			r := n.run()
			if r.err != nil {
				t.Fatalf("the script must not fail bashible: %v\n%s", r.err, r.stderr)
			}
			if _, ok := n.list(); ok || n.writes() != 0 {
				t.Fatalf("lvm.conf was changed although the root Volume Group is unknown\n%s", r.stderr)
			}
			if !strings.Contains(r.stderr, "left as it is") {
				t.Fatalf("the skip is not explained in the bashible log:\n%s", r.stderr)
			}
		})
	}
}

func TestAnUnresolvableFstabEntryIsReported(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.fstab = []string{"UUID=missing /srv ext4 defaults 0 0"}
	r := n.run()
	n.expectList(r, moduleTag)
	if !strings.Contains(r.stderr, "cannot find the device UUID=missing") {
		t.Fatalf("the unresolved fstab entry is not reported:\n%s", r.stderr)
	}
}

func TestAnInvalidNameFromTheCommandLineIsLeftOut(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.cmdline = `rd.lvm.vg=bad"name rd.lvm.vg=good`
	r := n.run()
	n.expectList(r, "good", moduleTag)
	if !strings.Contains(r.stderr, "is not a Volume Group name") {
		t.Fatalf("the invalid name is not reported:\n%s", r.stderr)
	}
}

func TestAnAdministratorsListIsExtendedNotReplaced(t *testing.T) {
	n := newFakeNode(t)
	n.disk("sda", "8:0")
	n.lv("dm-0", "253:0", "rootvg", "root", "sda")
	n.mount("253:0", "/", "ext4", "/dev/mapper/rootvg-root")
	n.setList(`["manual","vg2/lvol1","@site"]`)
	r := n.run()
	n.expectList(r, "manual", "vg2/lvol1", "@site", "rootvg", moduleTag)
	if !strings.Contains(r.stderr, "changed") {
		t.Fatalf("the change of an existing list is not reported:\n%s", r.stderr)
	}
}

func TestASingleStringListIsExtended(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.setList(`"manual"`)
	n.expectList(n.run(), "manual", moduleTag)
}

func TestAnEmptyListIsExtended(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.setList(`[]`)
	n.expectList(n.run(), moduleTag)
}

func TestACompleteListIsNotRewritten(t *testing.T) {
	n := newFakeNode(t)
	n.disk("sda", "8:0")
	n.lv("dm-0", "253:0", "rootvg", "root", "sda")
	n.mount("253:0", "/", "ext4", "/dev/mapper/rootvg-root")
	n.setList(`["` + moduleTag + `","manual","rootvg"]`)
	r := n.run()
	if r.err != nil {
		t.Fatalf("the script failed: %v\n%s", r.err, r.stderr)
	}
	if n.writes() != 0 {
		t.Fatalf("lvm.conf was rewritten although the list already has every entry\n%s", r.stderr)
	}
}

func TestTheUnboundModuleTagIsDropped(t *testing.T) {
	n := newFakeNode(t)
	n.disk("sda", "8:0")
	n.lv("dm-0", "253:0", "rootvg", "root", "sda")
	n.mount("253:0", "/", "ext4", "/dev/mapper/rootvg-root")
	n.setList(`["manual","rootvg","` + legacyTag + `"]`)
	n.expectList(n.run(), "manual", "rootvg", moduleTag)
}

func TestAnotherClustersTagIsReplaced(t *testing.T) {
	n := newFakeNode(t)
	n.rootOnPartition()
	n.setList(`["` + otherClusterTag + `","manual"]`)
	n.expectList(n.run(), "manual", moduleTag)
}

func TestWithoutAClusterUUIDTheListHasNoTag(t *testing.T) {
	for _, uuid := range []string{"", "not a tag", "a,b"} {
		t.Run(fmt.Sprintf("%q", uuid), func(t *testing.T) {
			n := newFakeNode(t)
			n.cluster = uuid
			n.disk("sda", "8:0")
			n.lv("dm-0", "253:0", "rootvg", "root", "sda")
			n.mount("253:0", "/", "ext4", "/dev/mapper/rootvg-root")
			n.setList(`["` + legacyTag + `"]`)
			n.expectList(n.run(), "rootvg")
		})
	}
}

func TestANodeWithNothingToAutoactivateGetsAnEmptyList(t *testing.T) {
	n := newFakeNode(t)
	n.cluster = ""
	n.rootOnPartition()
	n.setList(`["` + legacyTag + `"]`)
	r := n.run()
	n.expectList(r)
	raw, err := os.ReadFile(n.path("stubs/list"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(raw)); got != "auto_activation_volume_list=[]" {
		t.Fatalf("the list is written as %s, want an empty list", got)
	}
}

func TestASecondRunChangesNothing(t *testing.T) {
	n := newFakeNode(t)
	n.disk("sda", "8:0")
	n.lv("dm-0", "253:0", "rootvg", "root", "sda")
	n.mount("253:0", "/", "ext4", "/dev/mapper/rootvg-root")
	n.expectList(n.run(), "rootvg", moduleTag)
	r := n.run()
	n.expectList(r, "rootvg", moduleTag)
	if n.writes() != 1 {
		t.Fatalf("lvm.conf was written %d times over two runs, want once\n%s", n.writes(), r.stderr)
	}
}

func TestANodeWithoutLVMIsLeftAlone(t *testing.T) {
	t.Run("no lvmconfig", func(t *testing.T) {
		n := newFakeNode(t)
		n.rootOnPartition()
		if err := os.Remove(n.path("stubs/lvmconfig")); err != nil {
			t.Fatal(err)
		}
		bin := t.TempDir()
		for _, tool := range []string{"bash", "readlink", "cat", "grep", "tr", "sort", "tail", "mktemp", "rm", "mv", "stat"} {
			if p, err := exec.LookPath(tool); err == nil {
				if err := os.Symlink(p, filepath.Join(bin, tool)); err != nil {
					t.Fatal(err)
				}
			}
		}
		t.Setenv("PATH", bin)
		r := n.run()
		if r.err != nil {
			t.Fatalf("the script failed: %v\n%s", r.err, r.stderr)
		}
		if _, ok := n.list(); ok {
			t.Fatal("the list was written on a node without lvmconfig")
		}
	})
	t.Run("no lvm.conf", func(t *testing.T) {
		n := newFakeNode(t)
		n.rootOnPartition()
		if err := os.Remove(n.path("etc/lvm/lvm.conf")); err != nil {
			t.Fatal(err)
		}
		r := n.run()
		if r.err != nil {
			t.Fatalf("the script failed: %v\n%s", r.err, r.stderr)
		}
		if n.writes() != 0 {
			t.Fatal("lvmconfig was asked to write a config the node does not have")
		}
	})
}

func TestWithTheRealLvmconfig(t *testing.T) {
	if _, err := exec.LookPath("lvmconfig"); err != nil {
		t.Skip("lvmconfig is not installed")
	}
	n := newFakeNode(t)
	n.realLVM = true
	n.disk("sda", "8:0")
	n.lv("dm-0", "253:0", "ubuntu-vg", "ubuntu-lv", "sda")
	n.mount("253:0", "/", "ext4", "/dev/mapper/ubuntu--vg-ubuntu--lv")

	lvmconfig := func(node string) string {
		t.Helper()
		cmd := exec.Command("lvmconfig", node)
		cmd.Env = append(os.Environ(), "LVM_SYSTEM_DIR="+n.path("etc/lvm"))
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("lvmconfig %s: %v", node, err)
		}
		return strings.TrimSpace(string(out))
	}

	r := n.run()
	if r.err != nil {
		t.Fatalf("the script failed: %v\n%s", r.err, r.stderr)
	}
	if got, want := lvmconfig("activation/auto_activation_volume_list"), `auto_activation_volume_list=["ubuntu-vg","`+moduleTag+`"]`; got != want {
		t.Fatalf("lvm reads %s, want %s", got, want)
	}
	if got, want := lvmconfig("devices/global_filter"), `global_filter="r|^/dev/loop[0-9]+|"`; got != want {
		t.Fatalf("the rest of lvm.conf was not kept: %s, want %s", got, want)
	}

	first, err := os.ReadFile(n.path("etc/lvm/lvm.conf"))
	if err != nil {
		t.Fatal(err)
	}
	r = n.run()
	if r.err != nil {
		t.Fatalf("the second run failed: %v\n%s", r.err, r.stderr)
	}
	second, err := os.ReadFile(n.path("etc/lvm/lvm.conf"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("the second run changed lvm.conf:\n--- first\n%s\n--- second\n%s", first, second)
	}
}
