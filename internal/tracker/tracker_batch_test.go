package tracker

import (
	"testing"
)

// SB-TA-001/002 (agent side): an unacknowledged batch is re-sent
// verbatim — same ID, same content — and new traffic sampled in the
// meantime is never merged into it; it forms the next batch with a
// different ID, even when its content is identical.
func TestBatchResendVerbatimAndNoMerge(t *testing.T) {
	tr := New()

	tr.Process(map[int][2]int64{1: {100, 200}}, nil, 0)
	b1 := tr.FlushBatch()
	if b1 == nil {
		t.Fatal("expected a batch")
	}
	if b1.ID == "" {
		t.Fatal("batch must carry a persistent ID")
	}
	if got := b1.Data[1]; got != [2]int64{100, 200} {
		t.Fatalf("batch data = %v, want [100 200]", got)
	}
	if b1.CollectedFrom <= 0 || b1.CollectedTo < b1.CollectedFrom {
		t.Fatalf("collection window invalid: %+v", b1)
	}
	tr.SetUnacked(b1)

	// New traffic arrives while b1 is unacknowledged.
	tr.Process(map[int][2]int64{1: {150, 260}}, nil, 0)

	// The outstanding batch is still the verbatim original.
	out := tr.Unacked()
	if out == nil || out.ID != b1.ID {
		t.Fatalf("unacked batch changed: %+v", out)
	}
	if got := out.Data[1]; got != [2]int64{100, 200} {
		t.Fatalf("unacked batch was merged with new traffic: %v", got)
	}

	// Acknowledge b1; the accumulated traffic becomes a NEW batch
	// with a different ID containing only the new deltas.
	if !tr.AckBatch(b1.ID) {
		t.Fatal("AckBatch should accept the outstanding ID")
	}
	b2 := tr.FlushBatch()
	if b2 == nil {
		t.Fatal("expected a second batch")
	}
	if b2.ID == b1.ID {
		t.Fatal("second batch must have a different ID")
	}
	if got := b2.Data[1]; got != [2]int64{50, 60} {
		t.Fatalf("second batch data = %v, want [50 60] (deltas only)", got)
	}
}

// Two independent samples with identical content still get distinct
// batch IDs (SB-TA-002 agent side).
func TestBatchIDsUniqueForIdenticalContent(t *testing.T) {
	tr := New()

	tr.Process(map[int][2]int64{7: {10, 10}}, nil, 0)
	b1 := tr.FlushBatch()
	tr.Process(map[int][2]int64{7: {20, 20}}, nil, 0)
	b2 := tr.FlushBatch()

	if b1 == nil || b2 == nil {
		t.Fatal("expected two batches")
	}
	if b1.ID == b2.ID {
		t.Fatalf("identical-content batches share ID %q", b1.ID)
	}
	if b1.Data[7] != b2.Data[7] {
		t.Fatalf("test premise broken: contents should be identical, %v vs %v", b1.Data, b2.Data)
	}
}

// AckBatch with a foreign ID must not clear the outstanding batch.
func TestAckBatchRejectsForeignID(t *testing.T) {
	tr := New()
	tr.Process(map[int][2]int64{1: {1, 1}}, nil, 0)
	b := tr.FlushBatch()
	tr.SetUnacked(b)

	if tr.AckBatch("not-the-batch") {
		t.Fatal("AckBatch accepted a foreign ID")
	}
	if tr.Unacked() == nil {
		t.Fatal("outstanding batch was cleared by a foreign ack")
	}
}
