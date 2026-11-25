package tracker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
// atomically publishes a new snapshot. All read methods (Flush*,
// CurrentOnline, LogStats, *Speed) read the snapshot lock-free.
// This eliminates contention between the 10s Process tick and the 60s
// flush/push tick.
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

	// unacked is the last flushed batch that the panel has not yet
	// acknowledged. It is re-sent verbatim and never merged with new
	// samples. Protected by mu.
	unacked *Batch

	// batchSeq disambiguates batch IDs generated in the same process;
	// idPrefix is a per-process random component. Protected by mu.
	batchSeq uint64
	idPrefix string

	// live holds the current snapshot, swapped atomically.
	// Readers load this pointer without any lock.
	live atomic.Pointer[snapshot]

	// aliveIPsBuf is a reusable buffer for FlushAliveIPs output.
	// Avoids allocating a new map+slice every 60s.
	aliveIPsBuf map[int][]string

	// lastAliveIPsHash detects changes to avoid duplicate reports.
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
		aliveIPsBuf:    make(map[int][]string),
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

// SetUnacked records the batch awaiting panel acknowledgement.
func (t *Tracker) SetUnacked(b *Batch) {
	t.mu.Lock()
	t.unacked = b
	t.mu.Unlock()
}

// Unacked returns the batch awaiting acknowledgement, or nil.
func (t *Tracker) Unacked() *Batch {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.unacked
}

// AckBatch clears the unacknowledged batch when the panel confirmed
// receipt of exactly that batch. Returns false when the IDs differ
// (a newer batch is already in flight).
func (t *Tracker) AckBatch(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.unacked != nil && t.unacked.ID == id {
		t.unacked = nil
		return true
	}
	return false
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

// FlushAliveIPs returns per-user alive IPs.
// Reuses internal buffer. Returns nil if unchanged.
func (t *Tracker) FlushAliveIPs() map[int][]string {
	s := t.live.Load()

	t.mu.Lock()
	defer t.mu.Unlock()

	// Calculate hash of current aliveIPs
	currentHash := calcAliveIPsHash(s.aliveIPs)

	// If no changes, return nil to avoid duplicate reporting
	if currentHash == t.lastAliveIPsHash {
		return nil
	}

	t.lastAliveIPsHash = currentHash

	// Clear old buffer entries.
	for k := range t.aliveIPsBuf {
		delete(t.aliveIPsBuf, k)
	}

	// Fill buffer from snapshot.
	for uid, ips := range s.aliveIPs {
		buf := t.aliveIPsBuf[uid]
		if buf == nil {
			buf = make([]string, 0, len(ips))
		}
		buf = buf[:0]
		for ip := range ips {
			buf = append(buf, ip)
		}
		t.aliveIPsBuf[uid] = buf
	}

	return t.aliveIPsBuf
}

// calcAliveIPsHash computes a deterministic hash for change detection.
func calcAliveIPsHash(aliveIPs map[int]map[string]bool) string {
	if len(aliveIPs) == 0 {
		return ""
	}

	h := sha256.New()

	// Sort user IDs for consistent hashing
	userIDs := make([]int, 0, len(aliveIPs))
	for uid := range aliveIPs {
		userIDs = append(userIDs, uid)
	}
	sort.Ints(userIDs)

	for _, uid := range userIDs {
		ips := aliveIPs[uid]
		// Sort IPs for consistent hashing
		ipList := make([]string, 0, len(ips))
		for ip := range ips {
			ipList = append(ipList, ip)
		}
		sort.Strings(ipList)

		// Write user ID and IPs to hash
		h.Write([]byte{byte(uid >> 24), byte(uid >> 16), byte(uid >> 8), byte(uid)})
		for _, ip := range ipList {
			h.Write([]byte(ip))
		}
	}

	return hex.EncodeToString(h.Sum(nil))
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

// RestoreAliveIPs merges alive IPs back in (used when push to panel fails).
// Note: this operates on the buffer, which will be overwritten next Process().
func (t *Tracker) RestoreAliveIPs(data map[int][]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for uid, ipList := range data {
		ips := t.aliveIPsBuf[uid]
		if ips == nil {
			ips = make([]string, 0, len(ipList))
		}
		// Use map for O(n) dedup instead of O(n²) linear search
		existMap := make(map[string]struct{}, len(ips)+len(ipList))
		for _, existing := range ips {
			existMap[existing] = struct{}{}
		}
		for _, ip := range ipList {
			if _, exists := existMap[ip]; !exists {
				ips = append(ips, ip)
				existMap[ip] = struct{}{}
			}
		}
		t.aliveIPsBuf[uid] = ips
	}
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
