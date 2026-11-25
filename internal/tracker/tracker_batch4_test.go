package tracker

// SB-TA-010 regression tests: the unacknowledged traffic batch is
// persisted so a process restart resends it with its original ID
// instead of silently losing the last traffic window.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestUnackedBatchSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic-batch.json")

	t1 := New()
	t1.EnablePersistence(path)
	t1.Process(map[int][2]int64{7: {100, 200}, 9: {5, 6}}, nil, 0)
	b := t1.FlushBatch()
	if b == nil {
		t.Fatal("FlushBatch returned nil")
	}
	t1.SetUnacked(b)

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("persisted batch file missing: %v", err)
	}

	// Simulate a restart: a fresh tracker on the same path must
	// restore the batch verbatim — same ID, same window, same data.
	t2 := New()
	t2.EnablePersistence(path)
	u := t2.Unacked()
	if u == nil {
		t.Fatal("unacked batch lost across restart")
	}
	if u.ID != b.ID {
		t.Fatalf("restored batch ID = %q, want original %q (panel dedups on it)", u.ID, b.ID)
	}
	if u.CollectedFrom != b.CollectedFrom || u.CollectedTo != b.CollectedTo {
		t.Fatalf("restored window = [%d,%d], want [%d,%d]", u.CollectedFrom, u.CollectedTo, b.CollectedFrom, b.CollectedTo)
	}
	if len(u.Data) != 2 || u.Data[7] != [2]int64{100, 200} || u.Data[9] != [2]int64{5, 6} {
		t.Fatalf("restored data = %v, want user 7 {100 200} and user 9 {5 6}", u.Data)
	}

	// Acknowledgement clears both memory and disk.
	if !t2.AckBatch(b.ID) {
		t.Fatal("AckBatch returned false for the restored batch")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("batch file still present after ack (stat err = %v)", err)
	}
	t3 := New()
	t3.EnablePersistence(path)
	if t3.Unacked() != nil {
		t.Fatal("acked batch resurrected on a later restart")
	}
}

// Regression (SG pilot): in machine mode one node's per-node
// ConfigDir (…/instances/…/node-<id>/) was never created, so every
// persist failed with "no such file or directory" and that node's
// batches were never written. EnablePersistence must create the
// store directory itself.
func TestEnablePersistenceCreatesMissingStoreDir(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instances", "node-6", "traffic-batch-6.json")

	t1 := New()
	t1.EnablePersistence(path)
	if info, err := os.Stat(filepath.Dir(path)); err != nil || !info.IsDir() {
		t.Fatalf("store directory not created by EnablePersistence (err = %v)", err)
	}

	t1.Process(map[int][2]int64{3: {11, 22}}, nil, 0)
	b := t1.FlushBatch()
	if b == nil {
		t.Fatal("FlushBatch returned nil")
	}
	t1.SetUnacked(b)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("persisted batch file not readable: %v", err)
	}
	var bs batchStore
	if err := json.Unmarshal(raw, &bs); err != nil {
		t.Fatalf("persisted batch file undecodable: %v", err)
	}
	if bs.ID != b.ID || bs.CollectedFrom != b.CollectedFrom || bs.CollectedTo != b.CollectedTo ||
		len(bs.Data) != 1 || bs.Data[3] != [2]int64{11, 22} {
		t.Fatalf("persisted batch = %+v, want ID %q, window [%d,%d], user 3 {11 22}",
			bs, b.ID, b.CollectedFrom, b.CollectedTo)
	}
}

func TestCorruptPersistedBatchIsIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "traffic-batch.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	tr := New()
	tr.EnablePersistence(path)
	if tr.Unacked() != nil {
		t.Fatal("corrupt persisted batch must be ignored, not restored")
	}
}
