package tracker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

// Batch is one sampling window of per-user traffic deltas carrying a
// persistent, unique batch ID (SB-TA-001/002, agent side).
//
// Protocol: a batch is flushed once with its ID; until the panel
// acknowledges it (the report call succeeds), the SAME batch — same
// ID, same content — is re-sent verbatim. New traffic sampled in the
// meantime accumulates into the next batch and is never merged into
// an unacknowledged one. The panel dedups on (node, batch ID), so a
// retry cannot double-count old deltas (001) and two independent
// samples with identical content still count separately (002).
type Batch struct {
	ID            string
	CollectedFrom int64 // unix seconds: first delta of the window
	CollectedTo   int64 // unix seconds: flush time
	Data          map[int][2]int64
}

// snapshot is an immutable point-in-time view of tracker state.
// It is swapped atomically so readers never block writers.
type snapshot struct {
	traffic   map[int][2]int64        // userID → [upload, download] delta
	aliveIPs  map[int]map[string]bool // userID → set of source IPs
	online    map[int]int             // userID → distinct IP count
	connCount int
	inSpeed   int64
	outSpeed  int64
}

// Tracker computes per-user traffic deltas from cumulative counters
// provided by the kernel, and accumulates totals for panel reporting.
//
// Architecture: the kernel maintains per-user atomic counters and IP sets.
// Each tick, the service calls Process() with cumulative per-user traffic.
// Tracker computes deltas against the previous cycle's values — O(users),
// not O(connections).
//
// Thread safety: Process() acquires mu to update internal state, then
// atomically publishes a new snapshot. Observability reads use that
// snapshot lock-free. Traffic enqueue and acknowledgement use mu.
type Tracker struct {
	mu sync.Mutex

	// lastSeen stores the cumulative traffic from the previous Process() call.
	// Protected by mu — only written by Process.
	lastSeen map[int][2]int64 // userID → [upload, download] cumulative

	// pending accumulates deltas between flushes.
	// Protected by mu — written by Process, drained by FlushTraffic.
	pendingTraffic map[int][2]int64

	// pendingFrom is the unix time the first delta of the current
	// pending window landed (SB-TA-004 collection window).
	// Protected by mu.
	pendingFrom int64

	// queue holds immutable batches in send order. Protected by mu.
	queue     []*Batch
	storePath string

	// batchSeq disambiguates batch IDs generated in the same process;
	// idPrefix is a per-process random component. Protected by mu.
	batchSeq uint64
	idPrefix string

	// live holds the current snapshot, swapped atomically.
	// Readers load this pointer without any lock.
	live atomic.Pointer[snapshot]

	lastAliveIPsHash string
}

func New() *Tracker {
	var rb [8]byte
	idPrefix := fmt.Sprintf("%d", time.Now().UnixNano())
	if _, err := rand.Read(rb[:]); err == nil {
		idPrefix = hex.EncodeToString(rb[:])
	}
	t := &Tracker{
		lastSeen:       make(map[int][2]int64),
		pendingTraffic: make(map[int][2]int64),
		idPrefix:       idPrefix,
	}
	// Publish initial empty snapshot.
	t.live.Store(&snapshot{
		traffic:  make(map[int][2]int64),
		aliveIPs: make(map[int]map[string]bool),
		online:   make(map[int]int),
	})
	return t
}

// Process computes per-user traffic deltas from cumulative kernel counters.
// Also stores alive IPs and connection count. O(users).
//
// This is the only writer to lastSeen and pendingTraffic.
// After computing deltas, it publishes a new snapshot for lock-free reads.
func (t *Tracker) Process(
	cumTraffic map[int][2]int64,
	kernelAliveIPs map[int]map[string]bool,
	connCount int,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var cycleIn, cycleOut int64

	for uid, cum := range cumTraffic {
		prev := t.lastSeen[uid]
		deltaUp := cum[0] - prev[0]
		deltaDown := cum[1] - prev[1]

		// Guard against counter reset (kernel restart).
		if deltaUp < 0 {
			deltaUp = cum[0]
		}
		if deltaDown < 0 {
			deltaDown = cum[1]
		}

		t.lastSeen[uid] = cum

		if deltaUp > 0 || deltaDown > 0 {
			if len(t.pendingTraffic) == 0 && t.pendingFrom == 0 {
				t.pendingFrom = time.Now().Unix()
			}
			cur := t.pendingTraffic[uid]
			cur[0] += deltaUp
			cur[1] += deltaDown
			t.pendingTraffic[uid] = cur

			cycleOut += deltaUp
			cycleIn += deltaDown
		}
	}

	// Compute online from alive IPs.
	online := make(map[int]int, len(kernelAliveIPs))
	for uid, ips := range kernelAliveIPs {
		online[uid] = len(ips)
	}

	// Publish new snapshot (readers will see this atomically).
	t.live.Store(&snapshot{
		traffic:   copyTrafficMap(t.pendingTraffic),
		aliveIPs:  kernelAliveIPs, // kernel provides fresh copy each tick
		online:    online,
		connCount: connCount,
		inSpeed:   cycleIn,
		outSpeed:  cycleOut,
	})
}

// FlushTraffic returns accumulated per-user traffic and resets the pending buffer.
// Lock-free: reads from live snapshot, then acquires mu only to drain.
func (t *Tracker) FlushTraffic() map[int][2]int64 {
	t.mu.Lock()
	data := t.pendingTraffic
	t.pendingTraffic = make(map[int][2]int64, len(data))
	t.pendingFrom = 0
	t.mu.Unlock()
	return data
}

// FlushBatch drains the pending window into a new Batch with a fresh
// persistent ID (SB-TA-001/002). Returns nil when nothing accumulated.
// The caller is expected to SetUnacked the batch and re-send it
// verbatim until the panel acknowledges it.
func (t *Tracker) FlushBatch() *Batch {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushBatchLocked()
}

func (t *Tracker) flushBatchLocked() *Batch {
	if len(t.pendingTraffic) == 0 {
		return nil
	}
	data := t.pendingTraffic
	t.pendingTraffic = make(map[int][2]int64, len(data))
	from := t.pendingFrom
	t.pendingFrom = 0
	if from == 0 {
		from = time.Now().Unix()
	}
	t.batchSeq++
	return &Batch{
		ID:            fmt.Sprintf("%s-%d", t.idPrefix, t.batchSeq),
		CollectedFrom: from,
		CollectedTo:   time.Now().Unix(),
		Data:          data,
	}
}

// QueuePending seals and persists the current sampling window before sending.
func (t *Tracker) QueuePending() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if batch := t.flushBatchLocked(); batch != nil {
		t.queue = append(t.queue, batch)
	}
	if len(t.queue) == 0 {
		return nil
	}
	return t.persistQueueLocked()
}

// SetUnacked records a batch awaiting panel acknowledgement.
func (t *Tracker) SetUnacked(b *Batch) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if b != nil {
		t.queue = append(t.queue, b)
	}
	if err := t.persistQueueLocked(); err != nil {
		nlog.Core().Warn("tracker: failed to persist traffic queue", "error", err)
	}
}

func (t *Tracker) Unacked() *Batch {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.queue) == 0 {
		return nil
	}
	return t.queue[0]
}

// AckBatch removes only the head, after persisting the remaining queue.
func (t *Tracker) AckBatch(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.queue) == 0 || t.queue[0].ID != id {
		return false
	}
	previous := t.queue
	t.queue = t.queue[1:]
	if err := t.persistQueueLocked(); err != nil {
		t.queue = previous
		nlog.Core().Warn("tracker: failed to persist traffic acknowledgement", "error", err)
		return false
	}
	return true
}

type batchStore struct {
	ID            string           `json:"id"`
	CollectedFrom int64            `json:"collected_from"`
	CollectedTo   int64            `json:"collected_to"`
	Data          map[int][2]int64 `json:"data"`
}

type queueStore struct {
	batchStore
	Following []batchStore `json:"following,omitempty"`
}

func (t *Tracker) EnablePersistence(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.storePath = path
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		nlog.Core().Warn("tracker: failed to create traffic queue directory", "path", path, "error", err)
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		nlog.Core().Warn("tracker: failed to read traffic queue", "path", path, "error", err)
		return
	}
	var stored queueStore
	if err := json.Unmarshal(raw, &stored); err != nil || stored.ID == "" || len(stored.Data) == 0 {
		nlog.Core().Warn("tracker: ignoring unreadable persisted traffic queue", "path", path)
		return
	}
	for _, bs := range append([]batchStore{stored.batchStore}, stored.Following...) {
		t.queue = append(t.queue, &Batch{bs.ID, bs.CollectedFrom, bs.CollectedTo, bs.Data})
	}
}

func (t *Tracker) persistQueueLocked() error {
	if t.storePath == "" {
		return nil
	}
	dir := filepath.Dir(t.storePath)
	if len(t.queue) == 0 {
		if err := os.Remove(t.storePath); os.IsNotExist(err) {
			return nil
		} else if err != nil {
			return err
		}
	} else {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		stored := make([]batchStore, 0, len(t.queue))
		for _, b := range t.queue {
			stored = append(stored, batchStore{b.ID, b.CollectedFrom, b.CollectedTo, b.Data})
		}
		raw, err := json.Marshal(queueStore{stored[0], stored[1:]})
		if err != nil {
			return err
		}
		f, err := os.CreateTemp(dir, ".traffic-batch-*")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		_, writeErr := f.Write(raw)
		if writeErr == nil {
			writeErr = f.Sync()
		}
		if err := errors.Join(writeErr, f.Close()); err != nil {
			return err
		}
		if err := os.Rename(f.Name(), t.storePath); err != nil {
			return err
		}
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// RestoreTraffic adds traffic back (used when push to panel fails).
func (t *Tracker) RestoreTraffic(data map[int][2]int64) {
	t.mu.Lock()
	for uid, d := range data {
		cur := t.pendingTraffic[uid]
		cur[0] += d[0]
		cur[1] += d[1]
		t.pendingTraffic[uid] = cur
	}
	t.mu.Unlock()
}

// HasTraffic returns true if there is accumulated traffic to report.
func (t *Tracker) HasTraffic() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pendingTraffic) > 0
}

// FlushAliveIPs returns an independent snapshot if the legacy cursor changed.
func (t *Tracker) FlushAliveIPs() map[int][]string {
	t.mu.Lock()
	defer t.mu.Unlock()
	devices, hash := t.AliveIPs()
	if hash == t.lastAliveIPsHash {
		return nil
	}
	t.lastAliveIPsHash = hash
	return devices
}

// AliveIPs returns an independently owned full snapshot and its content hash.
func (t *Tracker) AliveIPs() (map[int][]string, string) {
	alive := t.live.Load().aliveIPs
	devices := make(map[int][]string, len(alive))
	if len(alive) == 0 {
		return devices, ""
	}
	userIDs := make([]int, 0, len(alive))
	for uid := range alive {
		userIDs = append(userIDs, uid)
	}
	sort.Ints(userIDs)
	h := sha256.New()
	var number [8]byte
	for _, uid := range userIDs {
		ips := make([]string, 0, len(alive[uid]))
		for ip := range alive[uid] {
			ips = append(ips, ip)
		}
		sort.Strings(ips)
		devices[uid] = ips
		binary.LittleEndian.PutUint64(number[:], uint64(uid))
		h.Write(number[:])
		binary.LittleEndian.PutUint64(number[:], uint64(len(ips)))
		h.Write(number[:])
		for _, ip := range ips {
			io.WriteString(h, ip)
			h.Write([]byte{0})
		}
	}
	return devices, hex.EncodeToString(h.Sum(nil))
}

// CurrentOnline returns a snapshot copy of user_id → device count (distinct IPs).
// Lock-free: reads from live snapshot.
func (t *Tracker) CurrentOnline() map[int]int {
	s := t.live.Load()
	cp := make(map[int]int, len(s.online))
	for k, v := range s.online {
		cp[k] = v
	}
	return cp
}

// LogStats logs current tracking statistics.
// Lock-free: reads from live snapshot.
func (t *Tracker) LogStats() {
	s := t.live.Load()
	nlog.TrackerStats(s.connCount, len(s.online))
}

// ActiveConnections returns the last observed active connection count.
// Lock-free: reads from live snapshot.
func (t *Tracker) ActiveConnections() int {
	return t.live.Load().connCount
}

// TotalConnections is deprecated — no longer tracked per-connection.
// Returns 0 for backward compatibility.
func (t *Tracker) TotalConnections() int64 {
	return 0
}

// InboundSpeed returns the last observed inbound (download) speed in bytes/second.
// Lock-free: reads from live snapshot.
func (t *Tracker) InboundSpeed() int64 {
	return t.live.Load().inSpeed / 10
}

// OutboundSpeed returns the last observed outbound (upload) speed in bytes/second.
// Lock-free: reads from live snapshot.
func (t *Tracker) OutboundSpeed() int64 {
	return t.live.Load().outSpeed / 10
}

// copyTrafficMap creates a shallow copy of the traffic map.
func copyTrafficMap(src map[int][2]int64) map[int][2]int64 {
	dst := make(map[int][2]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
