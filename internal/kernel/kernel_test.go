package kernel

import (
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

// NODE-013: a credential rotation (same ID, new UUID) must appear in
// BOTH diff lists — kernels keyed by an ID-derived identity cannot
// replace the credential by adding over the old one.
func TestUserDiffRotationListsBothSides(t *testing.T) {
	old := []model.UserSpec{{ID: 1, UUID: "uuid-a"}, {ID: 2, UUID: "uuid-2"}}
	next := []model.UserSpec{{ID: 1, UUID: "uuid-b"}, {ID: 3, UUID: "uuid-3"}}

	toAdd, toRemove := UserDiff(old, next)

	if len(toAdd) != 2 || len(toRemove) != 2 {
		t.Fatalf("toAdd = %v, toRemove = %v; want 2 each", toAdd, toRemove)
	}
	var addIDs, removeIDs []int
	for _, u := range toAdd {
		addIDs = append(addIDs, u.ID)
		if u.ID == 1 && u.UUID != "uuid-b" {
			t.Fatalf("toAdd user 1 UUID = %q, want the new uuid-b", u.UUID)
		}
	}
	for _, u := range toRemove {
		removeIDs = append(removeIDs, u.ID)
		if u.ID == 1 && u.UUID != "uuid-a" {
			t.Fatalf("toRemove user 1 UUID = %q, want the old uuid-a", u.UUID)
		}
	}
	if addIDs[0] != 1 || addIDs[1] != 3 || removeIDs[0] != 1 || removeIDs[1] != 2 {
		t.Fatalf("addIDs = %v removeIDs = %v, want [1 3] / [1 2]", addIDs, removeIDs)
	}
}

func TestUserDiffUnchangedSetIsEmpty(t *testing.T) {
	users := []model.UserSpec{{ID: 1, UUID: "uuid-a"}}
	toAdd, toRemove := UserDiff(users, users)
	if len(toAdd) != 0 || len(toRemove) != 0 {
		t.Fatalf("unchanged set diff = (%v, %v), want empty", toAdd, toRemove)
	}
}
