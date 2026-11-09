/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package asb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

// fakeNamespace is one namespace as the fake backend sees it.
type fakeNamespace struct {
	topics   map[string]bool
	entities int
}

// fakePoolBackend stands in for Azure. Namespaces are keyed by name, and a
// connection config carries the name in Namespace so calls can be routed.
type fakePoolBackend struct {
	mu         sync.Mutex
	namespaces map[string]*fakeNamespace
	created    []string
	locks      int
	onLock     func()

	state  map[string][]byte
	counts int      // exact counts asked of Azure
	ops    []string // mapping writes and topic creates, in order
	reads  []string // sessions read, in order
}

func newFakePoolBackend() *fakePoolBackend {
	return &fakePoolBackend{namespaces: map[string]*fakeNamespace{}, state: map[string][]byte{}}
}

func (f *fakePoolBackend) add(name string, entities int, topics ...string) {
	ns := &fakeNamespace{topics: map[string]bool{}, entities: entities}
	for _, topic := range topics {
		ns.topics[topic] = true
	}
	f.namespaces[name] = ns
}

func (f *fakePoolBackend) ns(cfg ConnectionConfig) *fakeNamespace {
	return f.namespaces[strings.TrimSuffix(cfg.Namespace, ".servicebus.windows.net")]
}

func (f *fakePoolBackend) namespaceReady(_ context.Context, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.namespaces[name]
	return ok, nil
}

func (f *fakePoolBackend) createNamespace(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, name)
	f.namespaces[name] = &fakeNamespace{topics: map[string]bool{}}
	return nil
}

func (f *fakePoolBackend) connectionConfig(_ context.Context, name string) (ConnectionConfig, error) {
	return ConnectionConfig{Namespace: name + ".servicebus.windows.net"}, nil
}

func (f *fakePoolBackend) topicExists(_ context.Context, cfg ConnectionConfig, topic string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ns(cfg).topics[topic], nil
}

func (f *fakePoolBackend) entities(_ context.Context, cfg ConnectionConfig) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts++
	return f.ns(cfg).entities, nil
}

func (f *fakePoolBackend) createTopic(_ context.Context, cfg ConnectionConfig, topic string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	ns := f.ns(cfg)
	ns.topics[topic] = true
	ns.entities++
	f.ops = append(f.ops, "create "+topic)
	return nil
}

func (f *fakePoolBackend) readState(_ context.Context, session string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads = append(f.reads, session)
	return f.state[session], nil
}

func (f *fakePoolBackend) writeState(_ context.Context, session string, state []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.state[session] = state
	f.ops = append(f.ops, "write "+session)
	return nil
}

func (f *fakePoolBackend) count(name string) int {
	counts := namespaceCounts{}
	_ = json.Unmarshal(f.state[placementLockSession], &counts)
	return counts[name]
}

func (f *fakePoolBackend) setCount(name string, n int) {
	counts := namespaceCounts{}
	_ = json.Unmarshal(f.state[placementLockSession], &counts)
	counts[name] = n
	f.state[placementLockSession], _ = json.Marshal(counts)
}

// home reads a tenant's home as stored: a namespace number, named here the
// way the pool names "egw-events" and its numbered namespaces.
func (f *fakePoolBackend) home(tenant string) string {
	for i := 0; ; i++ {
		state, ok := f.state[pieceSession(i)]
		if !ok {
			return ""
		}
		var piece storedPiece
		_ = json.Unmarshal(state, &piece)
		switch n := piece.Homes[tenant]; n {
		case 0:
		case 1:
			return "egw-events"
		default:
			return fmt.Sprintf("egw-events-%d", n)
		}
	}
}

func (f *fakePoolBackend) piece(i int) storedPiece {
	var piece storedPiece
	_ = json.Unmarshal(f.state[pieceSession(i)], &piece)
	return piece
}

func (f *fakePoolBackend) lock(context.Context) (placementLock, error) {
	f.mu.Lock()
	f.locks++
	onLock := f.onLock
	f.mu.Unlock()
	if onLock != nil {
		onLock()
	}
	return fakeLock{f}, nil
}

type fakeLock struct{ f *fakePoolBackend }

func (l fakeLock) state(ctx context.Context) ([]byte, error) {
	return l.f.readState(ctx, placementLockSession)
}

func (l fakeLock) setState(ctx context.Context, state []byte) error {
	return l.f.writeState(ctx, placementLockSession, state)
}

func (fakeLock) release() {}

func newTestPool(t *testing.T, backend *fakePoolBackend) *namespacePool {
	t.Helper()
	home := ConnectionConfig{Namespace: "egw-events.servicebus.windows.net"}
	pool, err := newNamespacePool(home, home.Namespace, backend)
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func TestEnsureChannels_NewChannelJoinsItsTenantsHomeWhileItHasRoom(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 10, "acme-orders")
	backend.add("egw-events-2", 10)
	backend.state[pieceSession(0)] = []byte(`{"homes":{"acme-v1":1}}`)
	pool := newTestPool(t, backend)

	placed, err := pool.ensureChannels(context.Background(), "acme-v1", []string{"acme-invoices", "acme-orders"})
	if err != nil {
		t.Fatal(err)
	}
	if placed["acme-invoices"].name != "egw-events" {
		t.Fatalf("a redeploy with a new channel put it in %s while home had room", placed["acme-invoices"].name)
	}
}

func TestEnsureChannels_NewTenantGoesToTheNewestNamespace(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	backend.add("egw-events-2", 10)
	pool := newTestPool(t, backend)

	placed, err := pool.ensureChannels(context.Background(), "siem-v1", []string{"siem-alerts"})
	if err != nil {
		t.Fatal(err)
	}
	if backend.home("siem-v1") != "egw-events-2" || placed["siem-alerts"].name != "egw-events-2" {
		t.Fatalf("placed in %q, want the newest namespace", backend.home("siem-v1"))
	}
	if len(backend.created) != 0 {
		t.Fatalf("created %v while the newest namespace had room", backend.created)
	}
}

func TestEnsureChannels_ExactlyEnoughRoomStillFits(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", placementLimit-snapshotGroupEntities-1)
	pool := newTestPool(t, backend)

	if _, err := pool.ensureChannels(context.Background(), "siem-v1", []string{"siem-alerts"}); err != nil {
		t.Fatal(err)
	}
	if len(backend.created) != 0 || backend.count("egw-events") != placementLimit {
		t.Fatalf("created %v, count %d; want the namespace filled to %d", backend.created, backend.count("egw-events"), placementLimit)
	}
}

func TestLookup_FindsANamespaceAnotherInstanceCreated(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	pool := newTestPool(t, backend)
	ctx := context.Background()

	if _, err := pool.lookup(ctx, "", "acme-orders"); !errors.Is(err, errNotPlaced) {
		t.Fatalf("want errNotPlaced before it exists, got %v", err)
	}
	backend.add("egw-events-2", 10, "acme-orders")

	m, err := pool.lookup(ctx, "", "acme-orders")
	if err != nil || m.name != "egw-events-2" {
		t.Fatalf("lookup = %q, %v; want the namespace created since", m.name, err)
	}
}

func TestLookup_StopsAtTheFirstMissingNamespace(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 10)
	backend.add("egw-events-3", 10, "stray")
	pool := newTestPool(t, backend)

	if _, err := pool.lookup(context.Background(), "", "stray"); !errors.Is(err, errNotPlaced) {
		t.Fatalf("a namespace after a gap was searched: %v", err)
	}
}

func TestNewNamespacePool_RejectsANameWithNoRoomForTheSuffix(t *testing.T) {
	if _, err := newNamespacePool(ConnectionConfig{}, strings.Repeat("a", 46)+".servicebus.windows.net", newFakePoolBackend()); err == nil {
		t.Fatal("a 46-character name was accepted; with -1000 it exceeds Azure's 50")
	}
	if _, err := newNamespacePool(ConnectionConfig{}, "egw-events.servicebus.windows.net", newFakePoolBackend()); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureChannels_NewTenantIsRecordedBeforeItsTopicsAreCreated(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	pool := newTestPool(t, backend)

	topics := []string{"acme-login", "acme-registration"}
	placed, err := pool.ensureChannels(context.Background(), "acme-v1", topics)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics {
		if placed[topic].name != "egw-events" || !backend.namespaces["egw-events"].topics[topic] {
			t.Fatalf("%s placed in %q, want egw-events", topic, placed[topic].name)
		}
	}
	if got := backend.home("acme-v1"); got != "egw-events" {
		t.Fatalf("mapping has acme-v1 in %q, want egw-events", got)
	}
	// 50 counted in Azure, 3 reserved for the snapshot group, 2 channels.
	if got := backend.count("egw-events"); got != 55 {
		t.Fatalf("count = %d, want 55", got)
	}

	firstCreate := slices.IndexFunc(backend.ops, func(op string) bool { return strings.HasPrefix(op, "create ") })
	for _, session := range []string{placementLockSession, pieceSession(0)} {
		if i := slices.Index(backend.ops, "write "+session); i < 0 || i > firstCreate {
			t.Fatalf("a topic was created before %s recorded it: %v", session, backend.ops)
		}
	}
}

func TestEnsureChannels_SpillsIntoTheNextNamespaceButTheSnapshotGroupStaysHome(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", placementLimit-6)
	pool := newTestPool(t, backend)
	ctx := context.Background()

	// Room for 6: the snapshot group's 3 and three channels, so the fourth spills.
	topics := []string{"acme-1", "acme-2", "acme-3", "acme-4"}
	placed, err := pool.ensureChannels(ctx, "acme-v1", topics)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics[:3] {
		if placed[topic].name != "egw-events" {
			t.Fatalf("%s placed in %s while home had room", topic, placed[topic].name)
		}
	}
	if placed["acme-4"].name != "egw-events-2" || !slices.Equal(backend.created, []string{"egw-events-2"}) {
		t.Fatalf("acme-4 placed in %s, created %v; want it in a new egw-events-2", placed["acme-4"].name, backend.created)
	}
	if backend.count("egw-events") != placementLimit || backend.count("egw-events-2") != 1 {
		t.Fatalf("counts = %d and %d, want %d and 1", backend.count("egw-events"), backend.count("egw-events-2"), placementLimit)
	}

	group, err := pool.ensureSnapshotGroup(ctx, "acme-v1", "acme-sync")
	if err != nil {
		t.Fatal(err)
	}
	if group.name != "egw-events" {
		t.Fatalf("snapshot group placed in %s, want the home namespace it was reserved in", group.name)
	}
}

func TestEnsureChannels_NewTenantSkipsANamespaceWithNoRoomForItsSnapshotGroup(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", placementLimit-3)
	pool := newTestPool(t, backend)

	// The snapshot group alone would fit, but then no channel could join it.
	placed, err := pool.ensureChannels(context.Background(), "acme-v1", []string{"acme-login"})
	if err != nil {
		t.Fatal(err)
	}
	if backend.home("acme-v1") != "egw-events-2" || placed["acme-login"].name != "egw-events-2" {
		t.Fatalf("home %q, acme-login in %q; want both in egw-events-2", backend.home("acme-v1"), placed["acme-login"].name)
	}
}

func TestNamespacePool_RestartFindsEveryTopicThroughTheMappingWithoutTheLock(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", placementLimit-4)
	ctx := context.Background()
	topics := []string{"acme-1", "acme-2"}

	if _, err := newTestPool(t, backend).ensureChannels(ctx, "acme-v1", topics); err != nil {
		t.Fatal(err)
	}
	locks := backend.locks

	restarted := newTestPool(t, backend)
	placed, err := restarted.ensureChannels(ctx, "acme-v1", topics)
	if err != nil {
		t.Fatal(err)
	}
	if placed["acme-1"].name != "egw-events" || placed["acme-2"].name != "egw-events-2" {
		t.Fatalf("after a restart acme-1 is in %q and acme-2 in %q", placed["acme-1"].name, placed["acme-2"].name)
	}
	group, err := restarted.ensureSnapshotGroup(ctx, "acme-v1", "acme-sync")
	if err != nil || group.name != "egw-events" {
		t.Fatalf("snapshot group = %q, %v; want egw-events", group.name, err)
	}
	if backend.locks != locks || len(backend.created) != 1 {
		t.Fatal("a restart placed the tenant again")
	}
}

func TestEnsureChannels_KeepsATenantPlacedBeforeTheMappingWhereItIs(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 40)
	backend.add("egw-events-2", 10, "acme-login")
	pool := newTestPool(t, backend)

	placed, err := pool.ensureChannels(context.Background(), "acme-v1", []string{"acme-login", "acme-registration"})
	if err != nil {
		t.Fatal(err)
	}
	if backend.home("acme-v1") != "egw-events-2" || placed["acme-registration"].name != "egw-events-2" {
		t.Fatalf("home %q, new channel in %q; want both in egw-events-2", backend.home("acme-v1"), placed["acme-registration"].name)
	}
	// Its snapshot group exists already, so nothing is reserved for one.
	if got := backend.count("egw-events-2"); got != 11 {
		t.Fatalf("count = %d, want the 10 in Azure plus the new channel", got)
	}
}

// Another instance placing the same tenant while this one waits must win, or
// a channel could end up in two namespaces.
func TestEnsureChannels_AdoptsAPlacementMadeWhileWaitingForTheLock(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	pool := newTestPool(t, backend)
	ctx := context.Background()

	backend.onLock = func() {
		backend.mu.Lock()
		backend.namespaces["egw-events"].topics["acme-login"] = true
		backend.mu.Unlock()
		_ = backend.writeState(ctx, pieceSession(0), []byte(`{"homes":{"acme-v1":1}}`))
	}

	placed, err := pool.ensureChannels(ctx, "acme-v1", []string{"acme-login"})
	if err != nil {
		t.Fatal(err)
	}
	if placed["acme-login"].name != "egw-events" {
		t.Fatalf("placed in %q, want the other instance's placement", placed["acme-login"].name)
	}
	if _, written := backend.state[placementLockSession]; written {
		t.Fatal("counted entities the other instance created")
	}
}

func TestEnsureChannels_NearTheLimitTheLargerOfStoredAndAzureCountsWins(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", placementLimit)
	backend.setCount("egw-events", placementLimit-50)
	pool := newTestPool(t, backend)

	if _, err := pool.ensureChannels(context.Background(), "acme-v1", []string{"acme-login"}); err != nil {
		t.Fatal(err)
	}
	if backend.home("acme-v1") != "egw-events-2" {
		t.Fatalf("placed in %q though Azure counts egw-events full", backend.home("acme-v1"))
	}
}

func TestEnsureChannels_FarFromTheLimitTheStoredCountIsUsed(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 5000)
	backend.setCount("egw-events", 100)
	pool := newTestPool(t, backend)

	if _, err := pool.ensureChannels(context.Background(), "acme-v1", []string{"acme-login"}); err != nil {
		t.Fatal(err)
	}
	if backend.counts != 0 || backend.count("egw-events") != 104 {
		t.Fatalf("listed the namespace %d times, count = %d; want no listing and 104", backend.counts, backend.count("egw-events"))
	}
}

func TestRelease_TakesDeletedEntitiesOffTheCountsAndForgetsTheTenant(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	pool := newTestPool(t, backend)
	ctx := context.Background()

	if _, err := pool.ensureChannels(ctx, "acme-v1", []string{"acme-login"}); err != nil {
		t.Fatal(err)
	}
	if err := pool.release(ctx, "acme-v1", map[string]int{"egw-events": 4}, true); err != nil {
		t.Fatal(err)
	}
	if backend.count("egw-events") != 50 || backend.home("acme-v1") != "" {
		t.Fatalf("count = %d, home = %q; want 50 and no entry", backend.count("egw-events"), backend.home("acme-v1"))
	}
	if home, err := pool.homeOf(ctx, "acme-v1"); err != nil || home != "" {
		t.Fatalf("the pool still remembers acme-v1 in %q (%v)", home, err)
	}
}

// nearlyFullPiece is a piece that fits under pieceSealBytes but would not with
// next added.
func nearlyFullPiece(t *testing.T, next string) storedPiece {
	t.Helper()
	piece := storedPiece{Homes: map[string]int{}}
	// {"homes":{...}} is 11 bytes plus 15 per "t000000/v1":1 and its comma.
	for i := range (pieceSealBytes - 11) / 15 {
		piece.Homes[fmt.Sprintf("t%06d/v1", i)] = 1
	}
	state, _ := json.Marshal(piece)
	piece.Homes[next] = 1
	grown, _ := json.Marshal(piece)
	delete(piece.Homes, next)
	if len(state) > pieceSealBytes || len(grown) <= pieceSealBytes {
		t.Fatalf("seed is %d bytes and %d with %s; want it to straddle %d", len(state), len(grown), next, pieceSealBytes)
	}
	return piece
}

func TestEnsureChannels_AFullPieceIsSealedAndTheTenantStartsTheNext(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	full := nearlyFullPiece(t, "acme-orders/v1")
	backend.state[pieceSession(0)], _ = json.Marshal(full)
	pool := newTestPool(t, backend)

	if _, err := pool.ensureChannels(context.Background(), "acme-orders/v1", []string{"acme-orders"}); err != nil {
		t.Fatal(err)
	}
	if first := backend.piece(0); !first.Sealed || len(first.Homes) != len(full.Homes) {
		t.Fatalf("piece 0 sealed=%v with %d tenants; want it sealed with its %d", first.Sealed, len(first.Homes), len(full.Homes))
	}
	if second := backend.piece(1); second.Sealed || len(second.Homes) != 1 || second.Homes["acme-orders/v1"] != 1 {
		t.Fatalf("piece 1 = %+v; want only the new tenant", second)
	}
	// The seal goes first, so no reader stops at piece 0 while piece 1 exists.
	sealed, started := slices.Index(backend.ops, "write "+pieceSession(0)), slices.Index(backend.ops, "write "+pieceSession(1))
	if sealed < 0 || started < sealed {
		t.Fatalf("writes in order %v; want piece 0 sealed before piece 1", backend.ops)
	}
}

func TestLoadHomes_ReadsUpToTheFirstPieceNotSealed(t *testing.T) {
	backend := newFakePoolBackend()
	backend.state[pieceSession(0)] = []byte(`{"sealed":true,"homes":{"acme/v1":1}}`)
	backend.state[pieceSession(1)] = []byte(`{"homes":{"siem/v1":2}}`)
	backend.state[pieceSession(2)] = []byte(`{"homes":{"stray/v1":1}}`)
	pool := newTestPool(t, backend)

	for tenant, want := range map[string]string{"acme/v1": "egw-events", "siem/v1": "egw-events-2"} {
		if home, err := pool.homeOf(context.Background(), tenant); err != nil || home != want {
			t.Fatalf("homeOf(%s) = %q, %v; want %s", tenant, home, err, want)
		}
	}
	if want := []string{pieceSession(0), pieceSession(1)}; !slices.Equal(backend.reads, want) {
		t.Fatalf("read %v; want %v", backend.reads, want)
	}
}

func TestHomeOf_FindsATenantPlacedInAPieceStartedSinceTheCacheWasFilled(t *testing.T) {
	backend := newFakePoolBackend()
	backend.state[pieceSession(0)] = []byte(`{"homes":{"acme/v1":1}}`)
	pool := newTestPool(t, backend)
	ctx := context.Background()

	if _, err := pool.homeOf(ctx, "acme/v1"); err != nil {
		t.Fatal(err)
	}
	// Another instance fills piece 0 and records the next tenant in piece 1.
	backend.state[pieceSession(0)] = []byte(`{"sealed":true,"homes":{"acme/v1":1}}`)
	backend.state[pieceSession(1)] = []byte(`{"homes":{"siem/v1":2}}`)

	if home, err := pool.homeOf(ctx, "siem/v1"); err != nil || home != "egw-events-2" {
		t.Fatalf("homeOf = %q, %v; want egw-events-2", home, err)
	}
}

// A piece emptied by deletes must stay sealed, or a starting instance would
// stop there and never read the pieces after it.
func TestRelease_ForgettingFromASealedPieceKeepsItSealed(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	backend.state[pieceSession(0)] = []byte(`{"sealed":true,"homes":{"acme/v1":1}}`)
	backend.state[pieceSession(1)] = []byte(`{"homes":{"siem/v1":1}}`)
	ctx := context.Background()

	if err := newTestPool(t, backend).release(ctx, "acme/v1", map[string]int{"egw-events": 4}, true); err != nil {
		t.Fatal(err)
	}
	if first := backend.piece(0); !first.Sealed || len(first.Homes) != 0 {
		t.Fatalf("piece 0 = %+v; want it sealed and empty", first)
	}
	if home, err := newTestPool(t, backend).homeOf(ctx, "siem/v1"); err != nil || home != "egw-events" {
		t.Fatalf("after the delete a starting instance finds siem/v1 in %q (%v)", home, err)
	}
}

// A tenant another instance deleted is placed afresh, not where a stale cache
// still has it.
func TestEnsureChannels_PlacesAgainATenantAnotherInstanceForgot(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	backend.add("egw-events-2", 10)
	backend.state[pieceSession(0)] = []byte(`{"homes":{"acme-v1":1}}`)
	pool := newTestPool(t, backend)
	ctx := context.Background()

	if home, err := pool.homeOf(ctx, "acme-v1"); err != nil || home != "egw-events" {
		t.Fatalf("homeOf = %q, %v; want egw-events", home, err)
	}
	backend.state[pieceSession(0)] = []byte(`{"homes":{}}`)

	if _, err := pool.ensureChannels(ctx, "acme-v1", []string{"acme-login"}); err != nil {
		t.Fatal(err)
	}
	if got := backend.home("acme-v1"); got != "egw-events-2" {
		t.Fatalf("mapping has acme-v1 in %q; want it placed afresh in the newest namespace", got)
	}
}

func TestLookup_DoesNotPlaceAnUnknownTopic(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 50)
	pool := newTestPool(t, backend)

	if _, err := pool.lookup(context.Background(), "acme-v1", "acme-login"); !errors.Is(err, errNotPlaced) {
		t.Fatalf("want errNotPlaced, got %v", err)
	}
	if backend.locks != 0 || len(backend.ops) != 0 {
		t.Fatalf("a lookup took the lock or wrote %v", backend.ops)
	}
}

func newPoolDriver(t *testing.T, backend *fakePoolBackend) *ASBBrokerDriver {
	t.Helper()
	useFakeCredential(t)
	return &ASBBrokerDriver{
		instanceID: "egw-test",
		compacted:  make(map[string]*compactedTopicState),
		pool:       newTestPool(t, backend),
		conns:      map[string]*nsConn{},
		topics:     map[string]*nsConn{},
	}
}

func TestPoolDriver_LookupOfAnUnknownTopicConnectsNothing(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 10)
	driver := newPoolDriver(t, backend)

	exists, err := driver.TopicExists(context.Background(), "siem-alerts")
	if err != nil || exists {
		t.Fatalf("TopicExists = %v, %v; want false, nil", exists, err)
	}
	err = driver.Publish(context.Background(), "siem-alerts", &connectors.Message{Value: []byte("x")})
	if !errors.Is(err, errNotPlaced) {
		t.Fatalf("Publish to an unplaced topic: want errNotPlaced, got %v", err)
	}
	if err := driver.DeleteTopics(context.Background(), []string{"siem-alerts"}); err != nil {
		t.Fatalf("deleting topics no namespace holds: %v", err)
	}
	if len(driver.conns) != 0 || driver.home != nil || backend.locks != 0 {
		t.Fatal("a lookup connected the driver or took the placement lock")
	}
}

func TestPoolDriver_ATenantSpanningNamespacesGetsAConnectionToEach(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", placementLimit-4)
	driver := newPoolDriver(t, backend)
	ctx := context.Background()

	// Room for the snapshot group and acme-1; acme-2 spills.
	metadata := map[string]map[string]string{
		"acme-1": {"apiName": "acme", "apiVersion": "v1"},
		"acme-2": {"apiName": "acme", "apiVersion": "v1"},
	}
	if err := driver.EnsureTopics(ctx, []string{"acme-1", "acme-2"}, metadata); err != nil {
		t.Fatal(err)
	}
	if driver.tenant != "acme/v1" || backend.home("acme/v1") != "egw-events" {
		t.Fatalf("tenant %q recorded in %q, want acme/v1 in egw-events", driver.tenant, backend.home("acme/v1"))
	}
	if len(driver.conns) != 2 || driver.topics["acme-1"].name != "egw-events" || driver.topics["acme-2"].name != "egw-events-2" {
		t.Fatalf("connections %v; want acme-1 on egw-events and acme-2 on egw-events-2", driver.conns)
	}

	home, err := driver.homeConn(ctx, "acme-sync", true)
	if err != nil {
		t.Fatal(err)
	}
	if home != driver.conns["egw-events"] {
		t.Fatal("the snapshot group did not reuse the home namespace's connection")
	}

	locks := backend.locks
	for topic, want := range map[string]string{"acme-1": "egw-events", "acme-2": "egw-events-2"} {
		c, err := driver.connFor(ctx, topic)
		if err != nil || c.name != want {
			t.Fatalf("connFor(%s) = %v, %v; want %s", topic, c, err, want)
		}
	}
	if backend.locks != locks {
		t.Fatal("a known topic was placed again")
	}
	if err := driver.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPoolDriver_ClosedDriverConnectsNothing(t *testing.T) {
	backend := newFakePoolBackend()
	backend.add("egw-events", 10, "siem-alerts")
	driver := newPoolDriver(t, backend)

	if err := driver.Close(); err != nil {
		t.Fatalf("closing an unplaced driver: %v", err)
	}
	if _, err := driver.connFor(context.Background(), "siem-alerts"); !errors.Is(err, errDriverClosed) {
		t.Fatalf("a closed driver looked a topic up: %v", err)
	}
	if err := driver.EnsureTopics(context.Background(), []string{"siem-alerts"}, nil); err == nil {
		t.Fatal("a closed driver placed topics")
	}
	if len(driver.conns) != 0 {
		t.Fatal("a closed driver built a client")
	}
}

func TestTenantKey(t *testing.T) {
	topics := []string{"b-topic", "a-topic"}
	if got := tenantKey(topics, map[string]map[string]string{"a-topic": {"apiName": "acme", "apiVersion": "v1"}}); got != "acme/v1" {
		t.Errorf("with metadata: %q, want acme/v1", got)
	}
	// Without metadata the key must not depend on the order topics arrive in.
	if got := tenantKey(topics, nil); got != "a-topic" {
		t.Errorf("without metadata: %q, want the first topic in order", got)
	}
}

func pagesOf[T any](pages []T, failAt int) *runtime.Pager[T] {
	next := 0
	return runtime.NewPager(runtime.PagingHandler[T]{
		More: func(T) bool { return next < len(pages) },
		Fetcher: func(context.Context, *T) (T, error) {
			var zero T
			if next == failAt {
				return zero, errors.New("page unavailable")
			}
			page := pages[next]
			next++
			return page, nil
		},
	})
}

type fakeLister struct {
	topics      []admin.ListTopicsResponse
	queues      []admin.ListQueuesResponse
	topicFailAt int
}

func (f fakeLister) NewListTopicsPager(*admin.ListTopicsOptions) *runtime.Pager[admin.ListTopicsResponse] {
	return pagesOf(f.topics, f.topicFailAt)
}

func (f fakeLister) NewListQueuesPager(*admin.ListQueuesOptions) *runtime.Pager[admin.ListQueuesResponse] {
	return pagesOf(f.queues, -1)
}

func topicPage(n int) admin.ListTopicsResponse {
	return admin.ListTopicsResponse{Topics: make([]admin.TopicItem, n)}
}

func queuePage(n int) admin.ListQueuesResponse {
	return admin.ListQueuesResponse{Queues: make([]admin.QueueItem, n)}
}

func TestCountEntities_ReadsEveryPageOfTopicsAndQueues(t *testing.T) {
	lister := fakeLister{
		topics:      []admin.ListTopicsResponse{topicPage(100), topicPage(100), topicPage(37)},
		queues:      []admin.ListQueuesResponse{queuePage(100), queuePage(4)},
		topicFailAt: -1,
	}
	got, err := countEntities(context.Background(), lister)
	if err != nil {
		t.Fatalf("countEntities: %v", err)
	}
	if got != 341 {
		t.Fatalf("count = %d, want 341", got)
	}
}

// A partial count would read as a namespace with more room than it has.
func TestCountEntities_FailsRatherThanReportingAPartialCount(t *testing.T) {
	lister := fakeLister{
		topics:      []admin.ListTopicsResponse{topicPage(100), topicPage(100)},
		queues:      []admin.ListQueuesResponse{queuePage(1)},
		topicFailAt: 1,
	}
	if _, err := countEntities(context.Background(), lister); err == nil {
		t.Fatal("expected a failed page to fail the count")
	}
}

func TestNamespaceHost(t *testing.T) {
	const key = "SharedAccessKey=c2VjcmV0"
	for _, tc := range []struct {
		cfg  ConnectionConfig
		want string
	}{
		{ConnectionConfig{Namespace: "tenants-02.servicebus.windows.net"}, "tenants-02.servicebus.windows.net"},
		{ConnectionConfig{ConnectionString: "Endpoint=sb://Tenants-01.ServiceBus.Windows.NET/;SharedAccessKeyName=root;" + key}, "tenants-01.servicebus.windows.net"},
		{ConnectionConfig{ConnectionString: "SharedAccessKeyName=root; Endpoint=sb://reordered.servicebus.windows.net;" + key}, "reordered.servicebus.windows.net"},
		{ConnectionConfig{ConnectionString: "SharedAccessKeyName=root;" + key}, ""},
	} {
		got := namespaceHost(tc.cfg)
		if got != tc.want {
			t.Errorf("namespaceHost = %q, want %q", got, tc.want)
		}
		if strings.Contains(got, "c2VjcmV0") {
			t.Errorf("namespaceHost leaked the key into a metric label: %q", got)
		}
	}
}
