package xray

// NODE-013 regression tests: rotating a user's UUID must revoke the
// old credential (same ID-derived email) before adding the new one,
// and any UserManager failure must surface as an error so the caller
// does not advance its snapshot over a kernel that still serves the
// old credential.

import (
	"context"
	"errors"
	"testing"

	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/uuid"
	"github.com/xtls/xray-core/proxy/vmess"

	"github.com/cedar2025/xboard-node/internal/model"
)

// fakeUserManager mimics xray's UserManager semantics: one account
// per email, AddUser fails when the email is already present,
// RemoveUser fails when it is absent.
type fakeUserManager struct {
	accounts map[string]string // email -> UUID
	failAdd  bool
}

func (f *fakeUserManager) AddUser(_ context.Context, mu *protocol.MemoryUser) error {
	if f.failAdd {
		return errors.New("add disabled")
	}
	if _, exists := f.accounts[mu.Email]; exists {
		return errors.New("user already exists")
	}
	acc, ok := mu.Account.(*vmess.MemoryAccount)
	if !ok {
		return errors.New("unexpected account type")
	}
	f.accounts[mu.Email] = acc.ID.String()
	return nil
}

func (f *fakeUserManager) RemoveUser(_ context.Context, email string) error {
	if _, exists := f.accounts[email]; !exists {
		return errors.New("user not found")
	}
	delete(f.accounts, email)
	return nil
}

func (f *fakeUserManager) GetUsers(_ context.Context) []*protocol.MemoryUser {
	var out []*protocol.MemoryUser
	for email := range f.accounts {
		if u := f.GetUser(context.Background(), email); u != nil {
			out = append(out, u)
		}
	}
	return out
}

func (f *fakeUserManager) GetUsersCount(_ context.Context) int64 {
	return int64(len(f.accounts))
}

func (f *fakeUserManager) GetUser(_ context.Context, email string) *protocol.MemoryUser {
	uuidStr, ok := f.accounts[email]
	if !ok {
		return nil
	}
	id, err := uuid.ParseString(uuidStr)
	if err != nil {
		return nil
	}
	return &protocol.MemoryUser{
		Email:   email,
		Account: &vmess.MemoryAccount{ID: protocol.NewID(id), Security: protocol.SecurityType_AUTO},
	}
}

const (
	uuidA = "11111111-1111-1111-1111-111111111111"
	uuidB = "22222222-2222-2222-2222-222222222222"
)

func TestApplyUserSyncUMRotationRevokesOldCredential(t *testing.T) {
	um := &fakeUserManager{accounts: map[string]string{userEmail(1): uuidA}}
	nc := &model.NodeSpec{Protocol: "vmess"}

	err := applyUserSyncUM(context.Background(), um, "vmess", nc,
		[]model.UserSpec{{ID: 1, UUID: uuidB}}, []model.UserSpec{{ID: 1, UUID: uuidA}})
	if err != nil {
		t.Fatalf("rotation sync error = %v, want nil (remove must precede add)", err)
	}
	if got := um.accounts[userEmail(1)]; got != uuidB {
		t.Fatalf("account for %s = %q, want the new credential %q", userEmail(1), got, uuidB)
	}
}

func TestApplyUserSyncUMAddWithoutRemoveCollides(t *testing.T) {
	// Control case proving the fake has xray's collision semantics:
	// adding over an existing email without removing first fails —
	// this is exactly what the old add-only diff did on rotation.
	um := &fakeUserManager{accounts: map[string]string{userEmail(1): uuidA}}
	nc := &model.NodeSpec{Protocol: "vmess"}
	err := applyUserSyncUM(context.Background(), um, "vmess", nc,
		[]model.UserSpec{{ID: 1, UUID: uuidB}}, nil)
	if err == nil {
		t.Fatal("add-over-existing must fail under xray UserManager semantics")
	}
	if got := um.accounts[userEmail(1)]; got != uuidA {
		t.Fatalf("account changed to %q despite failed add", got)
	}
}

func TestApplyUserSyncUMFailureIsReturned(t *testing.T) {
	um := &fakeUserManager{accounts: map[string]string{}, failAdd: true}
	nc := &model.NodeSpec{Protocol: "vmess"}
	err := applyUserSyncUM(context.Background(), um, "vmess", nc,
		[]model.UserSpec{{ID: 2, UUID: uuidA}}, nil)
	if err == nil {
		t.Fatal("UserManager failure must be returned so UpdateUsers can refuse to advance its snapshot")
	}
}
