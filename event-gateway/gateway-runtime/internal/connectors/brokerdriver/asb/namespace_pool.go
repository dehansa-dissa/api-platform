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
	"log/slog"
	"maps"
	"math/rand/v2"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/servicebus/armservicebus"
)

const (
	placementTimeout = 10 * time.Minute

	// Azure's longest namespace name, which "<first>-1000" must also fit.
	maxNamespaceNameLength = 50
	longestNameSuffix      = len("-1000")

	placementLockQueue   = "egw-namespace-placement"
	placementLockSession = "placement"
	placementLockRenew   = 20 * time.Second
	// A held session is refused after about 1.5 s, so a short wait keeps the
	// lock from sitting free while instances wait for it.
	placementRetryBase      = 500 * time.Millisecond
	placementRetryJitterMax = time.Second

	rootKeyName            = "RootManageSharedAccessKey"
	namespacePollFrequency = 10 * time.Second
	newTopicAttempts       = 6
	newTopicRetryDelay     = 10 * time.Second

	entityLimit = 10000

	// placementLimit stops short of Azure's limit so a count that is slightly
	// off never has a create refused.
	placementLimit = entityLimit - 10
	// A stored count this close to placementLimit is checked against Azure.
	recountWithin = 100
	// The sync topic and its -snapshot and -leader-lock queues.
	snapshotGroupEntities = 3

	// The mapping is session state on the placement lock queue.
	pieceSealBytes = 250 << 10
)

var errNotPlaced = errors.New("no namespace holds these topics")

// poolBackend is the Azure side of placement, replaced by a fake in tests.
type poolBackend interface {
	namespaceReady(ctx context.Context, name string) (bool, error)
	createNamespace(ctx context.Context, name string) error
	connectionConfig(ctx context.Context, name string) (ConnectionConfig, error)
	topicExists(ctx context.Context, cfg ConnectionConfig, topic string) (bool, error)
	entities(ctx context.Context, cfg ConnectionConfig) (int, error)
	createTopic(ctx context.Context, cfg ConnectionConfig, topic string) error
	lock(ctx context.Context) (placementLock, error)
	readState(ctx context.Context, session string) ([]byte, error)
	writeState(ctx context.Context, session string, state []byte) error
}

// placementLock is the held lock session, whose own state holds the counts.
type placementLock interface {
	state(ctx context.Context) ([]byte, error)
	setState(ctx context.Context, state []byte) error
	release()
}

type poolMember struct {
	name string
	cfg  ConnectionConfig
}

// namespacePool fills namespaces in order, creating "<first>-2", "<first>-3", ...
// A tenant's snapshot group stays in its home namespace; its channels spill to the
// newest one. A mapping on the placement lock queue records homes and counts.
type namespacePool struct {
	homeName string
	backend  poolBackend

	mu      sync.Mutex
	members []poolMember

	// homes caches the mapping's tenant -> snapshot namespace entries, and
	// homePiece which piece each sits in.
	homesMu     sync.Mutex
	homes       map[string]string
	homePiece   map[string]int
	newestPiece int

	// loadMu keeps drivers starting together to one read of the pieces.
	loadMu      sync.Mutex
	homesLoaded bool
}

// namespaceCounts is the lock session's state.
type namespaceCounts map[string]int

// storedPiece is one piece of the mapping: each tenant's home by its number.
// A sealed piece is full and has a successor.
type storedPiece struct {
	Sealed bool           `json:"sealed,omitempty"`
	Homes  map[string]int `json:"homes"`
}

func newNamespacePool(home ConnectionConfig, homeHost string, backend poolBackend) (*namespacePool, error) {
	homeName, _, err := splitNamespaceHost(homeHost)
	if err != nil {
		return nil, err
	}
	if len(homeName)+longestNameSuffix > maxNamespaceNameLength {
		return nil, fmt.Errorf("asb namespace %q must be at most %d characters to have numbered namespaces created beside it",
			homeName, maxNamespaceNameLength-longestNameSuffix)
	}
	return &namespacePool{
		homeName:  homeName,
		backend:   backend,
		members:   []poolMember{{name: homeName, cfg: home}},
		homes:     map[string]string{},
		homePiece: map[string]int{},
	}, nil
}

// memberNumber reverses memberName.
func (p *namespacePool) memberNumber(name string) (int, error) {
	if name == p.homeName {
		return 1, nil
	}
	if suffix, ok := strings.CutPrefix(name, p.homeName+"-"); ok {
		if n, err := strconv.Atoi(suffix); err == nil && n > 1 {
			return n, nil
		}
	}
	return 0, fmt.Errorf("namespace %s is not one of %s's numbered namespaces", name, p.homeName)
}

func (p *namespacePool) memberName(i int) string {
	if i == 1 {
		return p.homeName
	}
	return fmt.Sprintf("%s-%d", p.homeName, i)
}

// find looks in every known namespace and then in any created since. Any of a
// WebSubApi's topics may be new, so each is tried.
func (p *namespacePool) find(ctx context.Context, topics []string) (poolMember, bool, error) {
	p.mu.Lock()
	members := append([]poolMember(nil), p.members...)
	p.mu.Unlock()

	for searched := 0; ; {
		for _, m := range members[searched:] {
			for _, topic := range topics {
				exists, err := p.backend.topicExists(ctx, m.cfg, topic)
				if err != nil {
					return poolMember{}, false, fmt.Errorf("failed to look up topic %s in namespace %s: %w", topic, m.name, err)
				}
				if exists {
					return m, true, nil
				}
			}
		}
		searched = len(members)

		var err error
		if members, err = p.refresh(ctx); err != nil {
			return poolMember{}, false, err
		}
		if len(members) == searched {
			return poolMember{}, false, nil
		}
	}
}

// refresh appends namespaces another instance created.
func (p *namespacePool) refresh(ctx context.Context) ([]poolMember, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for {
		name := p.memberName(len(p.members) + 1)
		ready, err := p.backend.namespaceReady(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("failed to look up namespace %s: %w", name, err)
		}
		if !ready {
			return append([]poolMember(nil), p.members...), nil
		}
		cfg, err := p.backend.connectionConfig(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to namespace %s: %w", name, err)
		}
		p.members = append(p.members, poolMember{name: name, cfg: cfg})
	}
}

func (p *namespacePool) addNamespace(ctx context.Context) (poolMember, error) {
	p.mu.Lock()
	name := p.memberName(len(p.members) + 1)
	p.mu.Unlock()

	if err := p.backend.createNamespace(ctx, name); err != nil {
		return poolMember{}, fmt.Errorf("failed to create namespace %s: %w", name, err)
	}
	cfg, err := p.backend.connectionConfig(ctx, name)
	if err != nil {
		return poolMember{}, fmt.Errorf("failed to connect to namespace %s: %w", name, err)
	}

	member := poolMember{name: name, cfg: cfg}
	p.mu.Lock()
	p.members = append(p.members, member)
	p.mu.Unlock()
	slog.Info("Created namespace", "namespace", name)
	return member, nil
}

// placementRound is the counts read under one hold of the lock, and which of
// them have been checked against Azure during it.
type placementRound struct {
	counts  namespaceCounts
	checked map[string]bool
	changed bool
}

func (p *namespacePool) newRound(ctx context.Context, lock placementLock) (*placementRound, error) {
	state, err := lock.state(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read namespace counts: %w", err)
	}
	round := &placementRound{counts: namespaceCounts{}, checked: map[string]bool{}}
	if err := decodeMapping(placementLockSession, state, &round.counts); err != nil {
		return nil, err
	}
	return round, nil
}

func writeCounts(ctx context.Context, lock placementLock, counts namespaceCounts) error {
	state, err := encodeMapping(placementLockSession, counts)
	if err != nil {
		return err
	}
	if err := lock.setState(ctx, state); err != nil {
		return fmt.Errorf("failed to write namespace counts: %w", err)
	}
	return nil
}

func (r *placementRound) reserve(name string, n int) {
	r.counts[name] += n
	r.changed = true
}

// ensureChannels places and creates a tenant's missing channel topics under the
// lock: in its home namespace while it has room, then in the newest one.
func (p *namespacePool) ensureChannels(ctx context.Context, tenant string, topics []string) (map[string]poolMember, error) {
	home, err := p.homeOf(ctx, tenant)
	if err != nil {
		return nil, err
	}
	placed := map[string]poolMember{}
	missing, err := p.locateAll(ctx, home, topics, placed)
	if err != nil || len(missing) == 0 {
		return placed, err
	}

	ctx, cancel := context.WithTimeout(ctx, placementTimeout)
	defer cancel()
	lock, err := p.backend.lock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to take the namespace placement lock: %w", err)
	}
	defer lock.release()

	i, piece, home, err := p.pieceHolding(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if missing, err = p.locateAll(ctx, home, missing, placed); err != nil || len(missing) == 0 {
		return placed, err
	}

	round, err := p.newRound(ctx, lock)
	if err != nil {
		return nil, err
	}
	if home == "" {
		if home, err = p.homeFor(ctx, round, tenant, topics, placed, snapshotGroupEntities+1); err != nil {
			return nil, err
		}
		if err := p.addTenant(ctx, i, piece, tenant, home); err != nil {
			return nil, err
		}
	}
	homeMember, err := p.memberNamed(ctx, home)
	if err != nil {
		return nil, err
	}

	targets := make(map[string]poolMember, len(missing))
	for _, topic := range missing {
		target := homeMember
		room, err := p.hasRoom(ctx, round, target, 1)
		if err != nil {
			return nil, err
		}
		if !room {
			if target, err = p.newestWithRoom(ctx, round, 1); err != nil {
				return nil, err
			}
		}
		round.reserve(target.name, 1)
		targets[topic] = target
	}
	if err := writeCounts(ctx, lock, round.counts); err != nil {
		return nil, err
	}

	for _, topic := range missing {
		target := targets[topic]
		if err := p.backend.createTopic(ctx, target.cfg, topic); err != nil {
			return nil, fmt.Errorf("failed to create topic %s in namespace %s: %w", topic, target.name, err)
		}
		placed[topic] = target
		slog.Info("Created topic", "topic", topic, "namespace", target.name)
		if target.name != home {
			slog.Info("Tenant's home namespace is full; placed a channel topic in the newest one",
				"tenant", tenant, "home", home, "namespace", target.name, "topic", topic)
		}
	}
	return placed, nil
}

// ensureSnapshotGroup returns the tenant's home namespace. Its room was reserved
// at placement, so the caller creates the group there without the lock.
func (p *namespacePool) ensureSnapshotGroup(ctx context.Context, tenant, syncTopic string) (poolMember, error) {
	home, err := p.homeOf(ctx, tenant)
	if err != nil {
		return poolMember{}, err
	}
	if home != "" {
		return p.memberNamed(ctx, home)
	}

	ctx, cancel := context.WithTimeout(ctx, placementTimeout)
	defer cancel()
	lock, err := p.backend.lock(ctx)
	if err != nil {
		return poolMember{}, fmt.Errorf("failed to take the namespace placement lock: %w", err)
	}
	defer lock.release()

	i, piece, home, err := p.pieceHolding(ctx, tenant)
	if err != nil {
		return poolMember{}, err
	}
	if home != "" {
		return p.memberNamed(ctx, home)
	}

	placed := map[string]poolMember{}
	if _, err := p.locateAll(ctx, "", []string{syncTopic}, placed); err != nil {
		return poolMember{}, err
	}
	round, err := p.newRound(ctx, lock)
	if err != nil {
		return poolMember{}, err
	}
	if home, err = p.homeFor(ctx, round, tenant, []string{syncTopic}, placed, snapshotGroupEntities); err != nil {
		return poolMember{}, err
	}
	if round.changed {
		if err := writeCounts(ctx, lock, round.counts); err != nil {
			return poolMember{}, err
		}
	}
	if err := p.addTenant(ctx, i, piece, tenant, home); err != nil {
		return poolMember{}, err
	}
	return p.memberNamed(ctx, home)
}

// homeFor picks a home for an unmapped tenant: the namespace of its existing
// topics if any, else the newest namespace with room for need.
func (p *namespacePool) homeFor(ctx context.Context, round *placementRound, tenant string, topics []string, placed map[string]poolMember, need int) (string, error) {
	for _, topic := range topics {
		if m, ok := placed[topic]; ok {
			slog.Info("Added a tenant placed before the namespace mapping", "tenant", tenant, "namespace", m.name)
			return m.name, nil
		}
	}
	m, err := p.newestWithRoom(ctx, round, need)
	if err != nil {
		return "", err
	}
	round.reserve(m.name, snapshotGroupEntities)
	slog.Info("Placed tenant in a namespace", "tenant", tenant, "namespace", m.name)
	return m.name, nil
}

// newestWithRoom returns the newest namespace when it has room for need more
// entities, and otherwise creates the next one: older namespaces filled first.
func (p *namespacePool) newestWithRoom(ctx context.Context, round *placementRound, need int) (poolMember, error) {
	if _, err := p.refresh(ctx); err != nil {
		return poolMember{}, err
	}
	p.mu.Lock()
	newest := p.members[len(p.members)-1]
	p.mu.Unlock()

	room, err := p.hasRoom(ctx, round, newest, need)
	if err != nil || room {
		return newest, err
	}
	slog.Warn("Namespace is full; creating the next one", "full", newest.name, "entities", round.counts[newest.name])
	created, err := p.addNamespace(ctx)
	if err != nil {
		return poolMember{}, err
	}
	round.counts[created.name], round.checked[created.name] = 0, true
	round.changed = true
	return created, nil
}

// hasRoom reports whether m can take need more. Near the limit or when unknown,
// it counts in Azure once a round and trusts the larger of the two counts.
func (p *namespacePool) hasRoom(ctx context.Context, round *placementRound, m poolMember, need int) (bool, error) {
	count, known := round.counts[m.name]
	if !round.checked[m.name] && (!known || count+need > placementLimit-recountWithin) {
		exact, err := p.backend.entities(ctx, m.cfg)
		if err != nil {
			return false, fmt.Errorf("failed to count entities in namespace %s: %w", m.name, err)
		}
		round.checked[m.name] = true
		if exact > count || !known {
			count = max(count, exact)
			round.counts[m.name] = count
			round.changed = true
		}
	}
	return count+need <= placementLimit, nil
}

// lookup returns the namespace holding a tenant's topic without placing it.
func (p *namespacePool) lookup(ctx context.Context, tenant, topic string) (poolMember, error) {
	home := ""
	if tenant != "" {
		var err error
		if home, err = p.homeOf(ctx, tenant); err != nil {
			return poolMember{}, err
		}
	}
	m, found, err := p.locate(ctx, home, topic)
	if err != nil {
		return poolMember{}, err
	}
	if !found {
		return poolMember{}, errNotPlaced
	}
	return m, nil
}

// release takes deleted entities off the counts and, with forget, drops the
// tenant from the mapping.
func (p *namespacePool) release(ctx context.Context, tenant string, removed map[string]int, forget bool) error {
	ctx, cancel := context.WithTimeout(ctx, placementTimeout)
	defer cancel()
	lock, err := p.backend.lock(ctx)
	if err != nil {
		return fmt.Errorf("failed to take the namespace placement lock: %w", err)
	}
	defer lock.release()

	round, err := p.newRound(ctx, lock)
	if err != nil {
		return err
	}
	for name, n := range removed {
		if count, ok := round.counts[name]; ok {
			round.counts[name] = max(0, count-n)
		}
	}
	if err := writeCounts(ctx, lock, round.counts); err != nil {
		return err
	}
	if !forget {
		return nil
	}

	i, piece, home, err := p.pieceHolding(ctx, tenant)
	if err != nil || home == "" {
		return err
	}
	delete(piece.Homes, tenant)
	return p.writePiece(ctx, i, piece)
}

// locate finds a topic, trying the tenant's home namespace before the rest.
func (p *namespacePool) locate(ctx context.Context, home, topic string) (poolMember, bool, error) {
	if home != "" {
		m, err := p.memberNamed(ctx, home)
		if err != nil {
			return poolMember{}, false, err
		}
		exists, err := p.backend.topicExists(ctx, m.cfg, topic)
		if err != nil {
			return poolMember{}, false, fmt.Errorf("failed to look up topic %s in namespace %s: %w", topic, m.name, err)
		}
		if exists {
			return m, true, nil
		}
	}
	return p.find(ctx, []string{topic})
}

// locateAll records the namespace of each topic it finds in placed and
// returns the rest.
func (p *namespacePool) locateAll(ctx context.Context, home string, topics []string, placed map[string]poolMember) ([]string, error) {
	var missing []string
	for _, topic := range topics {
		m, found, err := p.locate(ctx, home, topic)
		if err != nil {
			return nil, err
		}
		if found {
			placed[topic] = m
		} else {
			missing = append(missing, topic)
		}
	}
	return missing, nil
}

func (p *namespacePool) memberNamed(ctx context.Context, name string) (poolMember, error) {
	if m, ok := p.known(name); ok {
		return m, nil
	}
	if _, err := p.refresh(ctx); err != nil {
		return poolMember{}, err
	}
	if m, ok := p.known(name); ok {
		return m, nil
	}
	return poolMember{}, fmt.Errorf("namespace %s named in the namespace mapping is not ready", name)
}

func (p *namespacePool) known(name string) (poolMember, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, m := range p.members {
		if m.name == name {
			return m, true
		}
	}
	return poolMember{}, false
}

// homeOf returns the namespace of a tenant's snapshot group, or "" if unplaced.
// A cache miss re-reads the newest pieces in case another instance placed it.
func (p *namespacePool) homeOf(ctx context.Context, tenant string) (string, error) {
	if err := p.loadHomes(ctx); err != nil {
		return "", err
	}
	if home, _, ok := p.cached(tenant); ok {
		return home, nil
	}
	if _, _, err := p.readPieces(ctx, p.newest()); err != nil {
		return "", err
	}
	home, _, _ := p.cached(tenant)
	return home, nil
}

// loadHomes reads the pieces once, in order up to the first one not sealed, so
// a starting instance learns where every tenant is without a read per tenant.
func (p *namespacePool) loadHomes(ctx context.Context) error {
	p.loadMu.Lock()
	defer p.loadMu.Unlock()
	if p.homesLoaded {
		return nil
	}
	if _, _, err := p.readPieces(ctx, 0); err != nil {
		return err
	}
	p.homesLoaded = true
	return nil
}

// readPieces reads piece from and each one after it up to the first not
// sealed, which is where a new tenant goes, and returns that piece.
func (p *namespacePool) readPieces(ctx context.Context, from int) (int, storedPiece, error) {
	for i := from; ; i++ {
		piece, err := p.readPiece(ctx, i)
		if err != nil || !piece.Sealed {
			return i, piece, err
		}
	}
}

// pieceHolding re-reads the piece recording tenant and returns it with its home,
// or the newest piece and "" for a tenant recorded nowhere.
func (p *namespacePool) pieceHolding(ctx context.Context, tenant string) (int, storedPiece, string, error) {
	if _, i, ok := p.cached(tenant); ok {
		piece, err := p.readPiece(ctx, i)
		if err != nil {
			return 0, storedPiece{}, "", err
		}
		if _, ok := piece.Homes[tenant]; ok {
			home, _, _ := p.cached(tenant)
			return i, piece, home, nil
		}
	}
	newest, piece, err := p.readPieces(ctx, p.newest())
	if err != nil {
		return 0, storedPiece{}, "", err
	}
	home, i, ok := p.cached(tenant)
	if !ok || i == newest {
		return newest, piece, home, nil
	}
	// Recorded in a piece sealed since, which readPieces read on its way past.
	if piece, err = p.readPiece(ctx, i); err != nil {
		return 0, storedPiece{}, "", err
	}
	return i, piece, home, nil
}

// addTenant records tenant in piece i, the newest. Past pieceSealBytes the seal is
// written first, so a failure leaves the tenant unplaced, never a piece unread.
func (p *namespacePool) addTenant(ctx context.Context, i int, piece storedPiece, tenant, home string) error {
	n, err := p.memberNumber(home)
	if err != nil {
		return err
	}
	homes := maps.Clone(piece.Homes)
	if homes == nil {
		homes = map[string]int{}
	}
	homes[tenant] = n
	next := storedPiece{Homes: homes}

	if len(piece.Homes) > 0 {
		state, err := encodeMapping(pieceSession(i), next)
		if err != nil {
			return err
		}
		if len(state) > pieceSealBytes {
			piece.Sealed = true
			if err := p.writePiece(ctx, i, piece); err != nil {
				return err
			}
			i, next = i+1, storedPiece{Homes: map[string]int{tenant: n}}
			slog.Info("Namespace mapping piece is full; sealed it and started the next",
				"sealed", pieceSession(i-1), "tenants", len(piece.Homes))
		}
	}
	return p.writePiece(ctx, i, next)
}

// readPiece reads piece i afresh and refreshes the cache from it. One never
// written reads as empty and unsealed.
func (p *namespacePool) readPiece(ctx context.Context, i int) (storedPiece, error) {
	session := pieceSession(i)
	state, err := p.backend.readState(ctx, session)
	if err != nil {
		return storedPiece{}, fmt.Errorf("failed to read namespace mapping %s: %w", session, err)
	}
	var piece storedPiece
	if err := decodeMapping(session, state, &piece); err != nil {
		return storedPiece{}, err
	}
	return piece, p.cachePiece(i, piece)
}

// writePiece stores piece i and refreshes the cache from it.
func (p *namespacePool) writePiece(ctx context.Context, i int, piece storedPiece) error {
	session := pieceSession(i)
	state, err := encodeMapping(session, piece)
	if err != nil {
		return err
	}
	if err := p.backend.writeState(ctx, session, state); err != nil {
		return fmt.Errorf("failed to write namespace mapping %s: %w", session, err)
	}
	return p.cachePiece(i, piece)
}

// cachePiece makes the cache agree with piece i: its tenants are added, and any
// the cache had in it that it no longer holds are dropped.
func (p *namespacePool) cachePiece(i int, piece storedPiece) error {
	homes := make(map[string]string, len(piece.Homes))
	for tenant, n := range piece.Homes {
		if n < 1 {
			return fmt.Errorf("namespace mapping %s has tenant %s in namespace %d", pieceSession(i), tenant, n)
		}
		homes[tenant] = p.memberName(n)
	}

	p.homesMu.Lock()
	defer p.homesMu.Unlock()
	for tenant, at := range p.homePiece {
		if _, kept := homes[tenant]; at == i && !kept {
			delete(p.homes, tenant)
			delete(p.homePiece, tenant)
		}
	}
	for tenant, home := range homes {
		p.homes[tenant], p.homePiece[tenant] = home, i
	}
	newest := i
	if piece.Sealed {
		newest = i + 1
	}
	p.newestPiece = max(p.newestPiece, newest)
	return nil
}

// cached returns a tenant's home and the piece it was read from.
func (p *namespacePool) cached(tenant string) (string, int, bool) {
	p.homesMu.Lock()
	defer p.homesMu.Unlock()
	home, ok := p.homes[tenant]
	return home, p.homePiece[tenant], ok
}

func (p *namespacePool) newest() int {
	p.homesMu.Lock()
	defer p.homesMu.Unlock()
	return p.newestPiece
}

func decodeMapping(session string, state []byte, out any) error {
	if len(state) == 0 {
		return nil
	}
	if err := json.Unmarshal(state, out); err != nil {
		return fmt.Errorf("failed to decode namespace mapping %s: %w", session, err)
	}
	return nil
}

func encodeMapping(session string, value any) ([]byte, error) {
	state, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("failed to encode namespace mapping %s: %w", session, err)
	}
	return state, nil
}

func pieceSession(piece int) string {
	return fmt.Sprintf("tenants-%02d", piece)
}

type entityLister interface {
	NewListTopicsPager(options *admin.ListTopicsOptions) *runtime.Pager[admin.ListTopicsResponse]
	NewListQueuesPager(options *admin.ListQueuesOptions) *runtime.Pager[admin.ListQueuesResponse]
}

var _ entityLister = (*admin.Client)(nil)

// countEntities totals topics and queues, the two kinds the namespace quota
// counts. The management API reports no total, so every page is read.
func countEntities(ctx context.Context, lister entityLister) (int, error) {
	count := 0
	topics := lister.NewListTopicsPager(nil)
	for topics.More() {
		page, err := topics.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to list topics: %w", err)
		}
		count += len(page.Topics)
	}
	queues := lister.NewListQueuesPager(nil)
	for queues.More() {
		page, err := queues.NextPage(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to list queues: %w", err)
		}
		count += len(page.Queues)
	}
	return count, nil
}

// splitNamespaceHost splits "name.servicebus.windows.net" into the namespace
// name and the ".servicebus.windows.net" suffix.
func splitNamespaceHost(host string) (string, string, error) {
	name, rest, ok := strings.Cut(host, ".")
	if !ok || name == "" {
		return "", "", fmt.Errorf("asb namespace pool needs a fully qualified namespace, got %q", host)
	}
	return name, "." + rest, nil
}

// namespaceHost names the namespace a config points at.
func namespaceHost(cfg ConnectionConfig) string {
	if cfg.Namespace != "" {
		return cfg.Namespace
	}
	for _, part := range strings.Split(cfg.ConnectionString, ";") {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || !strings.EqualFold(key, "Endpoint") {
			continue
		}
		endpoint, err := url.Parse(value)
		if err != nil {
			return ""
		}
		return strings.ToLower(endpoint.Hostname())
	}
	return ""
}

var (
	poolsMu sync.Mutex
	pools   = map[string]*namespacePool{}
)

// sharedPool returns one pool per configured namespace for the whole process,
// since there is a driver per WebSubApi.
func sharedPool(cfg ConnectionConfig) (*namespacePool, error) {
	homeHost := namespaceHost(cfg)
	key := strings.Join([]string{homeHost, cfg.SubscriptionID, cfg.ResourceGroup}, "|")

	poolsMu.Lock()
	defer poolsMu.Unlock()

	if pool, ok := pools[key]; ok {
		return pool, nil
	}
	backend, err := newAzurePoolBackend(cfg, homeHost)
	if err != nil {
		return nil, err
	}
	pool, err := newNamespacePool(cfg, homeHost, backend)
	if err != nil {
		return nil, err
	}
	pools[key] = pool
	return pool, nil
}

type azurePoolBackend struct {
	home          ConnectionConfig
	homeName      string
	suffix        string
	resourceGroup string
	namespaces    *armservicebus.NamespacesClient
	homeClient    *azservicebus.Client

	mu     sync.Mutex
	admins map[string]*admin.Client

	lockQueueMu    sync.Mutex
	lockQueueReady bool
}

var _ poolBackend = (*azurePoolBackend)(nil)

func newAzurePoolBackend(home ConnectionConfig, homeHost string) (*azurePoolBackend, error) {
	homeName, suffix, err := splitNamespaceHost(homeHost)
	if err != nil {
		return nil, err
	}
	// Creating a namespace is an Azure management call, which only accepts an
	// Entra ID login, however the driver reaches the namespaces themselves.
	cred, err := sharedCredential()
	if err != nil {
		return nil, err
	}
	namespaces, err := armservicebus.NewNamespacesClient(home.SubscriptionID, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create azure namespaces client: %w", err)
	}
	homeClient, err := NewClient(home)
	if err != nil {
		return nil, fmt.Errorf("failed to create asb client for the placement lock: %w", err)
	}
	return &azurePoolBackend{
		home:          home,
		homeName:      homeName,
		suffix:        suffix,
		resourceGroup: home.ResourceGroup,
		namespaces:    namespaces,
		homeClient:    homeClient,
		admins:        map[string]*admin.Client{},
	}, nil
}

func (b *azurePoolBackend) namespaceReady(ctx context.Context, name string) (bool, error) {
	resp, err := b.namespaces.Get(ctx, b.resourceGroup, name, nil)
	if err != nil {
		if isNotFoundErr(err) {
			return false, nil
		}
		return false, err
	}
	if resp.Properties == nil || resp.Properties.ProvisioningState == nil {
		return false, nil
	}
	return strings.EqualFold(*resp.Properties.ProvisioningState, "Succeeded"), nil
}

// createNamespace copies the first namespace's region and tier.
func (b *azurePoolBackend) createNamespace(ctx context.Context, name string) error {
	home, err := b.namespaces.Get(ctx, b.resourceGroup, b.homeName, nil)
	if err != nil {
		return fmt.Errorf("failed to read namespace %s in resource group %s: %w", b.homeName, b.resourceGroup, err)
	}
	poller, err := b.namespaces.BeginCreateOrUpdate(ctx, b.resourceGroup, name, armservicebus.SBNamespace{
		Location: home.Location,
		SKU:      home.SKU,
		Tags: map[string]*string{
			"created-by":     to.Ptr("wso2-event-gateway"),
			"namespace-pool": to.Ptr(b.homeName),
		},
	}, nil)
	if err != nil {
		return err
	}
	_, err = poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: namespacePollFrequency})
	return err
}

// connectionConfig reaches a created namespace the way the first one is
// reached: through Entra ID when namespace is set, otherwise with its own key.
func (b *azurePoolBackend) connectionConfig(ctx context.Context, name string) (ConnectionConfig, error) {
	cfg := b.home
	if b.home.Namespace != "" {
		cfg.Namespace = name + b.suffix
		return cfg, nil
	}
	keys, err := b.namespaces.ListKeys(ctx, b.resourceGroup, name, rootKeyName, nil)
	if err != nil {
		return ConnectionConfig{}, fmt.Errorf("failed to read the %s key: %w", rootKeyName, err)
	}
	if keys.PrimaryConnectionString == nil {
		return ConnectionConfig{}, fmt.Errorf("namespace %s returned no connection string", name)
	}
	cfg.ConnectionString = *keys.PrimaryConnectionString
	return cfg, nil
}

func (b *azurePoolBackend) adminFor(cfg ConnectionConfig) (*admin.Client, error) {
	host := namespaceHost(cfg)

	b.mu.Lock()
	defer b.mu.Unlock()

	if client, ok := b.admins[host]; ok {
		return client, nil
	}
	client, err := newAdminClient(cfg, nil)
	if err != nil {
		return nil, err
	}
	b.admins[host] = client
	return client, nil
}

func (b *azurePoolBackend) topicExists(ctx context.Context, cfg ConnectionConfig, topic string) (bool, error) {
	client, err := b.adminFor(cfg)
	if err != nil {
		return false, err
	}
	return topicExists(ctx, client, topic)
}

func (b *azurePoolBackend) entities(ctx context.Context, cfg ConnectionConfig) (int, error) {
	client, err := b.adminFor(cfg)
	if err != nil {
		return 0, err
	}
	return countEntities(ctx, client)
}

// createTopic retries because a namespace that has just been provisioned can
// refuse calls until its DNS name resolves.
func (b *azurePoolBackend) createTopic(ctx context.Context, cfg ConnectionConfig, topic string) error {
	client, err := b.adminFor(cfg)
	if err != nil {
		return err
	}
	for attempt := 1; ; attempt++ {
		if _, err = createTopic(ctx, client, topic); err == nil {
			return nil
		}
		if attempt == newTopicAttempts || !sleepFor(ctx, newTopicRetryDelay) {
			return err
		}
	}
}

// ensureLockQueue creates the session queue in the first namespace that the
// lock and the mapping live on.
func (b *azurePoolBackend) ensureLockQueue(ctx context.Context) error {
	b.lockQueueMu.Lock()
	defer b.lockQueueMu.Unlock()
	if b.lockQueueReady {
		return nil
	}
	homeAdmin, err := b.adminFor(b.home)
	if err != nil {
		return err
	}
	if _, err := homeAdmin.CreateQueue(ctx, placementLockQueue, &admin.CreateQueueOptions{
		Properties: &admin.QueueProperties{RequiresSession: to.Ptr(true)},
	}); err != nil && !isConflictErr(err) {
		return fmt.Errorf("failed to create placement lock queue: %w", err)
	}
	b.lockQueueReady = true
	return nil
}

// acceptSession takes a session on the placement lock queue, waiting its turn:
// Service Bus refuses a session another receiver holds instead of queueing.
func (b *azurePoolBackend) acceptSession(ctx context.Context, session string) (*azservicebus.SessionReceiver, error) {
	if err := b.ensureLockQueue(ctx); err != nil {
		return nil, err
	}
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, compactionElectionAttemptTimeout)
		receiver, err := b.homeClient.AcceptSessionForQueue(attemptCtx, placementLockQueue, session, nil)
		cancel()
		if err == nil {
			return receiver, nil
		}
		if !isSessionHeldErr(err) && !isSBTimeoutErr(err) {
			return nil, err
		}
		if !sleepFor(ctx, placementRetryBase+rand.N(placementRetryJitterMax)) {
			return nil, ctx.Err()
		}
	}
}

func (b *azurePoolBackend) readState(ctx context.Context, session string) ([]byte, error) {
	receiver, err := b.acceptSession(ctx, session)
	if err != nil {
		return nil, err
	}
	defer closeSession(receiver)
	return receiver.GetSessionState(ctx, nil)
}

func (b *azurePoolBackend) writeState(ctx context.Context, session string, state []byte) error {
	receiver, err := b.acceptSession(ctx, session)
	if err != nil {
		return err
	}
	defer closeSession(receiver)
	return receiver.SetSessionState(ctx, state, nil)
}

func closeSession(receiver *azservicebus.SessionReceiver) {
	ctx, cancel := context.WithTimeout(context.Background(), sessionRenewTimeout)
	defer cancel()
	_ = receiver.Close(ctx)
}

// lock holds a named session on a queue in the first namespace; Service Bus
// grants a session to one receiver at a time.
func (b *azurePoolBackend) lock(ctx context.Context) (placementLock, error) {
	session, err := b.acceptSession(ctx, placementLockSession)
	if err != nil {
		return nil, err
	}

	renewCtx, stopRenew := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for sleepFor(renewCtx, placementLockRenew) {
			callCtx, cancel := context.WithTimeout(renewCtx, sessionRenewTimeout)
			err := session.RenewSessionLock(callCtx, nil)
			cancel()
			if err != nil && renewCtx.Err() == nil {
				slog.Warn("Failed to renew the namespace placement lock", "error", err)
			}
		}
	}()

	return &sessionLock{session: session, stop: func() {
		stopRenew()
		<-done
	}}, nil
}

type sessionLock struct {
	session *azservicebus.SessionReceiver
	stop    func()
}

func (l *sessionLock) release() {
	l.stop()
	closeSession(l.session)
}

func (l *sessionLock) state(ctx context.Context) ([]byte, error) {
	return l.session.GetSessionState(ctx, nil)
}

func (l *sessionLock) setState(ctx context.Context, state []byte) error {
	return l.session.SetSessionState(ctx, state, nil)
}
