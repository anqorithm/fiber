package session

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/extractors"
	"github.com/gofiber/fiber/v3/internal/apptest"
	"github.com/gofiber/fiber/v3/internal/storage/memory"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// sessionTestResolve returns the session ID that store reads from ctx and where
// it came from, failing the test if the extractor errors.
func sessionTestResolve(t *testing.T, store *Store, ctx fiber.Ctx) extractors.Result {
	t.Helper()

	from, err := store.getSessionID(ctx)
	require.NoError(t, err)
	return from
}

func Test_Store_getSessionID(t *testing.T) {
	t.Parallel()
	expectedID := "test-session-id"
	app := fiber.New()
	plantCookie := func(ctx fiber.Ctx, _ string) { ctx.Request().Header.SetCookie("session_id", "abc123") }
	skipping := extractors.Chain(extractors.Extractor{}, extractors.FromCookie("session_id"))

	for _, tc := range []struct {
		store *Store
		plant func(ctx fiber.Ctx, key string) // puts the ID where the store looks for it; key is the extractor key
		name  string
		want  string
	}{
		{
			name:  "from cookie",
			store: NewStore(),
			plant: func(ctx fiber.Ctx, key string) { ctx.Request().Header.SetCookie(key, expectedID) },
			want:  expectedID,
		},
		{
			name:  "from header",
			store: NewStore(Config{Extractor: extractors.FromHeader("session_id")}),
			plant: func(ctx fiber.Ctx, key string) { ctx.Request().Header.Set(key, expectedID) },
			want:  expectedID,
		},
		{
			name:  "from url query",
			store: NewStore(Config{Extractor: extractors.FromQuery("session_id")}),
			plant: func(ctx fiber.Ctx, key string) {
				ctx.Request().SetRequestURI(fmt.Sprintf("/path?%s=%s", key, expectedID))
			},
			want: expectedID,
		},
		// A chain carrying a zero-value child, which Chain skips but the store used
		// to call straight through, panicking on the nil function.
		{
			name:  "skips children without Extract",
			store: NewStore(Config{Extractor: skipping}),
			plant: plantCookie,
			want:  "abc123",
		},
		// A Store built directly rather than through NewStore, which would have
		// replaced the zero-value extractor with the default. There is nothing to
		// run, so there is no session ID.
		{name: "without extractor", store: &Store{}, plant: plantCookie},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := apptest.Ctx(t, app)
			tc.plant(ctx, tc.store.Extractor.Key)

			from := sessionTestResolve(t, tc.store, ctx)
			require.Equal(t, tc.want, from.Value)
		})
	}
}

// Regression: https://github.com/gofiber/fiber/issues/1408
// Regression: https://github.com/gofiber/fiber/security/advisories/GHSA-98j2-3j3p-fw2v
func Test_Store_Get(t *testing.T) {
	t.Parallel()
	unexpectedID := "test-session-id"
	app := fiber.New()
	t.Run("session should be re-generated if it is invalid", func(t *testing.T) {
		t.Parallel()
		store := NewStore()
		ctx := apptest.Ctx(t, app)
		ctx.Request().Header.SetCookie(store.Extractor.Key, unexpectedID)

		acquiredSession, err := store.Get(ctx)
		require.NoError(t, err)

		require.NotEqual(t, unexpectedID, acquiredSession.ID())
	})
}

// An ID from the request must not reach an entry another middleware keeps in
// the same Storage, even one whose bytes decode as session data.
func Test_Store_SharedStorage_ForeignRecord(t *testing.T) {
	t.Parallel()
	// where an earlier cache version kept a response body
	const foreignKey = "v5|GET|scheme=http|host=example.com|/files/0|q=|h=accept:0|accept-encoding:0|accept-language:0_body"

	var forged bytes.Buffer
	require.NoError(t, gob.NewEncoder(&forged).Encode(&map[any]any{"user": "admin"}))

	store := NewStore()
	require.NoError(t, store.Storage.Set(foreignKey, forged.Bytes(), 0))

	sess := loadSession(t, fiber.New(), store, foreignKey)
	require.True(t, sess.Fresh())
	require.NotEqual(t, foreignKey, sess.ID())
	require.Nil(t, sess.Get("user"))
	require.NoError(t, sess.Save())

	sessionTestRequireGone(t, store, foreignKey)

	require.NoError(t, store.Delete(context.Background(), foreignKey))
	kept, err := store.Storage.Get(foreignKey)
	require.NoError(t, err)
	require.Equal(t, forged.Bytes(), kept)
}

// A session can outlive its request, so its ID must not point into the request
// buffer the connection's next request reuses.
func Test_Store_SessionIDOutlivesRequest(t *testing.T) {
	t.Parallel()
	app := fiber.New()
	store := NewStore()
	id := seedSession(t, app, store, func(s *Session) { s.Set("user", "alice") })

	loaders := map[string]func(fiber.Ctx) (*Session, error){
		"Get":     store.Get,
		"GetByID": func(c fiber.Ctx) (*Session, error) { return store.GetByID(c, c.Cookies("session_id")) },
	}
	for name, load := range loaders {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fctx := &fasthttp.RequestCtx{}
			fctx.Request.Header.SetCookie("session_id", id)
			ctx := app.AcquireCtx(fctx)
			sess, err := load(ctx)
			app.ReleaseCtx(ctx)
			require.NoError(t, err)
			defer sess.Release()

			fctx.Request.Reset()
			fctx.Request.Header.SetCookie("session_id", strings.Repeat("A", len(id)))
			require.Equal(t, id, sess.ID())
		})
	}
}

func Test_Store_DeleteSession(t *testing.T) {
	t.Parallel()
	app := fiber.New()
	store := NewStore()
	ctx := apptest.Ctx(t, app)

	session, err := store.Get(ctx)
	require.NoError(t, err)
	sessionID := session.ID()
	require.NoError(t, store.Delete(ctx, sessionID))

	session, err = store.Get(ctx)
	require.NoError(t, err)

	// The session ID should be different now, because the old session was deleted
	require.NotEqual(t, sessionID, session.ID())
}

// conventions:ignore parallel: legacy, sequential before the rule; make it parallel and delete this line
func TestStore_Get_SessionAlreadyLoaded(t *testing.T) {
	app := fiber.New()
	ctx := apptest.Ctx(t, app)

	// Mock middleware and set it in the context
	middleware := &Middleware{}
	ctx.Locals(middlewareContextKey, middleware)

	store := &Store{}
	sess, err := store.Get(ctx)
	require.Nil(t, sess)
	require.Equal(t, ErrSessionAlreadyLoadedByMiddleware, err)
}

// conventions:ignore parallel: legacy, sequential before the rule; make it parallel and delete this line
func TestStore_Delete(t *testing.T) {
	store := NewStore()

	t.Run("delete with empty session ID", func(t *testing.T) {
		err := store.Delete(context.Background(), "")
		require.Error(t, err)
		require.Equal(t, ErrEmptySessionID, err)
	})

	t.Run("delete non-existing session", func(t *testing.T) {
		require.NoError(t, store.Delete(context.Background(), "non-existing-session-id"))
	})
}

func Test_Store_GetByID(t *testing.T) {
	t.Parallel()
	store := NewStore()

	for _, tc := range []struct {
		err  error
		name string
		id   string
	}{
		{name: "empty session ID", err: ErrEmptySessionID},
		{name: "nonexistent session ID", id: "nonexistent-session-id", err: ErrSessionIDNotFoundInStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess, err := store.GetByID(context.Background(), tc.id)
			require.Error(t, err)
			require.Nil(t, sess)
			require.Equal(t, tc.err, err)
		})
	}

	t.Run("valid session ID", func(t *testing.T) {
		t.Parallel()
		ctx := apptest.Ctx(t, fiber.New())
		session, err := store.Get(ctx)
		defer session.Release()
		require.NoError(t, err)
		sessionID := session.ID()
		require.NoError(t, session.Save())

		retrievedSession, err := store.GetByID(context.Background(), sessionID)
		require.NoError(t, err)
		require.NotNil(t, retrievedSession)
		require.Equal(t, sessionID, retrievedSession.ID())

		retrievedSession.Set("key", "value")
		require.NoError(t, retrievedSession.Save())
		require.Equal(t, "value", retrievedSession.Get("key"))
		require.False(t, retrievedSession.Fresh())

		require.NoError(t, retrievedSession.Reset())
		require.NoError(t, retrievedSession.Destroy())
		require.IsType(t, []any{}, retrievedSession.Keys())
		require.NoError(t, retrievedSession.Regenerate())
		require.NotPanics(t, func() { retrievedSession.Release() })
	})
}

// Test_Store_getSessionID_HonorsChainLevelExtract pins that a chain-level
// Extract runs. The store used to walk Extractor.Chain itself, calling the
// children directly, so a decorator wrapping a chain was silently skipped.
func Test_Store_getSessionID_HonorsChainLevelExtract(t *testing.T) {
	t.Parallel()
	app := fiber.New()

	t.Run("decoration runs and children do not run twice", func(t *testing.T) {
		t.Parallel()
		var childCalls, overrideCalls int

		base := extractors.Chain(
			extractors.FromCookie("session_id"),
			extractors.FromCustom("probe", func(fiber.Ctx) (string, error) {
				childCalls++
				return "raw-id", nil
			}),
		)

		// Value-preserving, as the store requires: the ID is written back
		// untransformed, so a decorator that rewrote it on read would never
		// find its own session again.
		decorated := base
		decorated.Extract = func(c fiber.Ctx) (string, error) {
			overrideCalls++
			v, err := base.Extract(c)
			if err != nil {
				return "", err
			}
			return v, nil
		}

		store := NewStore(Config{Extractor: decorated})
		ctx := apptest.Ctx(t, app)

		from := sessionTestResolve(t, store, ctx)

		require.Equal(t, "raw-id", from.Value, "the chain-level Extract must produce the ID")
		require.Equal(t, 1, overrideCalls, "the override must run exactly once")
		require.Equal(t, 1, childCalls, "the children must run once, through the override")
		require.Equal(t, extractors.SourceCustom, from.Source, "the winning child must be reported")
		require.Equal(t, "probe", from.Key)
	})

	t.Run("a rejecting decorator refuses the id", func(t *testing.T) {
		t.Parallel()
		base := extractors.Chain(extractors.FromHeader("X-Session"))

		// A validator refusing an ID it does not like. Skipping it silently
		// accepted the raw value.
		decorated := base
		decorated.Extract = func(c fiber.Ctx) (string, error) {
			v, err := base.Extract(c)
			if err != nil {
				return "", err
			}
			if v != "trusted" {
				return "", extractors.ErrNotFound
			}
			return v, nil
		}

		store := NewStore(Config{Extractor: decorated})

		ctx := apptest.Ctx(t, app)
		ctx.Request().Header.Set("X-Session", "attacker-supplied")
		from := sessionTestResolve(t, store, ctx)
		require.Empty(t, from.Value, "the validator must be able to refuse an ID")

		ok := apptest.Ctx(t, app)
		ok.Request().Header.Set("X-Session", "trusted")
		acceptedFrom := sessionTestResolve(t, store, ok)
		require.Equal(t, "trusted", acceptedFrom.Value)
		require.Equal(t, extractors.SourceHeader, acceptedFrom.Source)
		require.Equal(t, "X-Session", acceptedFrom.Key)
	})
}

// Test_Store_getSessionID_ReportsWinnerForWriteBack pins that the reported
// extractor is the one that supplied the ID, which decides where it is written
// back.
func Test_Store_getSessionID_ReportsWinnerForWriteBack(t *testing.T) {
	t.Parallel()
	app := fiber.New()
	chain := extractors.Chain(extractors.FromCookie("sid_cookie"), extractors.FromHeader("X-Sid"))

	for _, tc := range []struct {
		plant func(ctx fiber.Ctx) // puts the request in the state under test; nil leaves it blank
		name  string
		want  extractors.Result
	}{
		{
			name:  "cookie wins",
			plant: func(ctx fiber.Ctx) { ctx.Request().Header.SetCookie("sid_cookie", "from-cookie") },
			want:  extractors.Result{Value: "from-cookie", Key: "sid_cookie", Source: extractors.SourceCookie, Resolved: true},
		},
		{
			name:  "header wins when the cookie is absent",
			plant: func(ctx fiber.Ctx) { ctx.Request().Header.Set("X-Sid", "from-header") },
			want:  extractors.Result{Value: "from-header", Key: "X-Sid", Source: extractors.SourceHeader, Resolved: true},
		},
		{name: "nothing matches", want: extractors.Result{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewStore(Config{Extractor: chain})
			ctx := apptest.Ctx(t, app)
			if tc.plant != nil {
				tc.plant(ctx)
			}

			require.Equal(t, tc.want, sessionTestResolve(t, store, ctx))
		})
	}
}

// Test_Store_FixationGuard pins the session-fixation guard: an ID that reaches a
// store through a read-only source loads the existing session it names, and
// saving that session must not pin the ID into a cookie, whichever way the
// extractor hides where the ID came from.
func Test_Store_FixationGuard(t *testing.T) {
	t.Parallel()
	// A chain-level Extract answering on its own records no child, so Resolve
	// reports the chain's declared metadata — its first child. Reading that as
	// provenance would classify a query-supplied ID as "came from a writable
	// sink" and pin the victim to an attacker-chosen session.
	base := extractors.Chain(
		extractors.FromCookie("sid"), // declared metadata: cookie, a writable sink
		extractors.FromQuery("sid"),
	)
	attackable := base
	attackable.Extract = func(c fiber.Ctx) (string, error) {
		return fiber.Query[string](c, "sid"), nil // reads the query, delegates to no child
	}

	// The guard keys off whether an ID arrived with the request, not off Key. A
	// keyless child — FromCustom(""), or a hand-rolled one — used to skip the
	// guard entirely and let a read-only ID reach the configured cookie sink.
	keyless := extractors.Chain(
		extractors.FromCustom("", func(c fiber.Ctx) (string, error) { return fiber.Query[string](c, "sid"), nil }),
		extractors.FromCookie("sid"),
	)

	for _, tc := range []struct {
		name      string
		msg       string
		extractor extractors.Extractor
	}{
		{
			name:      "unattributable ID",
			extractor: attackable,
			msg:       "an ID whose origin cannot be attributed must never be pinned into a cookie",
		},
		{
			name:      "keyless extractor",
			extractor: keyless,
			msg:       "a read-only ID must not be pinned into a cookie just because its extractor has no Key",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewStore(Config{Extractor: tc.extractor})
			app := fiber.New()

			// Seed a real session, so the ID the attacker plants names an existing one.
			plantedID := seedSession(t, app, store, nil)

			// The victim's request carries that ID in the query and no cookie.
			ctx := apptest.Ctx(t, app)
			ctx.Request().SetRequestURI("/p?sid=" + plantedID)

			sess, err := store.Get(ctx)
			require.NoError(t, err)
			require.False(t, sess.Fresh(), "the planted ID should load the existing session")
			require.NoError(t, sess.Save())

			require.NotContains(t, string(ctx.Response().Header.Peek(fiber.HeaderSetCookie)), plantedID, tc.msg)
		})
	}
}

// Test_Store_ForeignChainDoesNotSupplyProvenance pins that a leaf consulting
// some other chain is not credited with that chain's winner, which would let
// the session ID be written into an unrelated application cookie.
func Test_Store_ForeignChainDoesNotSupplyProvenance(t *testing.T) {
	t.Parallel()
	tenant := extractors.Chain(extractors.FromHeader("X-Tenant"), extractors.FromCookie("tenant"))
	leaf := extractors.FromCustom("sid", func(c fiber.Ctx) (string, error) {
		//nolint:errcheck // the lookup's outcome is irrelevant; it runs only so a foreign chain records a winner
		tenant.Extract(c) // an unrelated lookup, in the same Resolve frame
		return fiber.Query[string](c, "sid"), nil
	})

	store := NewStore(Config{Extractor: leaf})
	ctx := apptest.Ctx(t, fiber.New())
	ctx.Request().SetRequestURI("/p?sid=planted")
	ctx.Request().Header.SetCookie("tenant", "acme")

	from := sessionTestResolve(t, store, ctx)
	require.Equal(t, "planted", from.Value)
	require.Equal(t, "sid", from.Key, "the leaf must not inherit the tenant chain's winner")
	require.Equal(t, extractors.SourceCustom, from.Source)
}

// Test_Store_NestedChainCookieSinkIsWritten pins that a cookie extractor nested
// inside an inner chain is still written back. getExtractorInfo used to range
// over direct children and judge each by its static Source, so an inner
// Chain(query, cookie) looked like a query extractor: no Set-Cookie was emitted
// and every request began a session that could never be resumed.
func Test_Store_NestedChainCookieSinkIsWritten(t *testing.T) {
	t.Parallel()
	store := NewStore(Config{
		Extractor: extractors.Chain(
			extractors.Chain(
				extractors.FromQuery("session_id"),  // first child: a read-only source
				extractors.FromCookie("session_id"), // the sink that must still be found
			),
		),
	})

	ctx := apptest.Ctx(t, fiber.New())

	sess, err := store.Get(ctx)
	require.NoError(t, err)
	require.True(t, sess.Fresh())
	require.NoError(t, sess.Save())

	require.Contains(t, string(ctx.Response().Header.Peek(fiber.HeaderSetCookie)), sess.ID(),
		"a cookie sink nested one level down must still be written")
}

// Test_Store_ExtractorErrorReachesCaller pins that a validator's refusal is
// reported rather than collapsed into "no session ID present" — a forged ID and
// a first-time visitor must not look alike.
func Test_Store_ExtractorErrorReachesCaller(t *testing.T) {
	t.Parallel()
	errForged := errors.New("forged session id")

	base := extractors.Chain(extractors.FromCookie("session_id"))
	validating := base
	validating.Extract = func(c fiber.Ctx) (string, error) {
		v, err := base.Extract(c)
		if err != nil {
			return "", err // absent: still an ordinary ErrNotFound
		}
		if v != "trusted" {
			return "", errForged
		}
		return v, nil
	}

	store := NewStore(Config{Extractor: validating})
	app := fiber.New()

	t.Run("a refusal is reported", func(t *testing.T) {
		t.Parallel()
		ctx := apptest.Ctx(t, app)
		ctx.Request().Header.SetCookie("session_id", "forged-value")

		_, err := store.Get(ctx)
		require.ErrorIs(t, err, errForged, "the validator's rejection must reach the caller")
	})

	t.Run("an absent id is not an error", func(t *testing.T) {
		t.Parallel()
		ctx := apptest.Ctx(t, app)

		sess, err := store.Get(ctx)
		require.NoError(t, err, "a first-time visitor must not look like a failure")
		require.True(t, sess.Fresh())
	})
}

// Test_Store_CachedIDsDoNotLeakAcrossRequests runs requests of random Store.Get calls over four
// stores back to back on request contexts that are reset between them, as fasthttp does, on several
// goroutines at once, and checks what each request holds at its end: one entry for each store that
// issued an ID, and nothing from another request. An entry that went back to the pool twice, or
// stayed in the list of a request after it was reclaimed, shows up as an ID this request never
// issued, or a store twice.
func Test_Store_CachedIDsDoNotLeakAcrossRequests(t *testing.T) {
	t.Parallel()
	app := fiber.New()
	stores := make([]*Store, 4)
	for i := range stores {
		stores[i] = NewStore(Config{Extractor: extractors.FromCookie("sid" + strconv.Itoa(i))})
	}

	const workers, requests = 4, 300
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			fctx := &fasthttp.RequestCtx{}
			c := app.AcquireCtx(fctx)
			defer app.ReleaseCtx(c)
			rng := rand.New(rand.NewPCG(uint64(worker), 7))

			for request := range requests {
				fctx.ResetUserValues() // the end of the previous request: fasthttp closes what it holds
				issued := make(map[string]*Store)
				for range 1 + rng.IntN(8) {
					store := stores[rng.IntN(len(stores))]
					sess, err := store.Get(c)
					if err != nil {
						t.Error(err)
						return
					}
					if sess.Fresh() {
						issued[sess.ID()] = store
					}
					if rng.IntN(2) == 0 {
						if saveErr := sess.Save(); saveErr != nil {
							t.Error(saveErr)
							return
						}
					}
					sess.Release()
				}

				seen := make(map[*Store]bool)
				for entry := requestIDs(c); entry != nil; entry = entry.next {
					switch {
					case seen[entry.store]:
						t.Errorf("worker %d request %d: a store has two entries", worker, request)
					case issued[entry.id] != entry.store:
						t.Errorf("worker %d request %d: entry %q was not issued by its store in this request", worker, request, entry.id)
					}
					seen[entry.store] = true
				}
			}
		})
	}
	wg.Wait()
}

// armedFailureStorage fails its reads once armed.
type armedFailureStorage struct {
	fiber.Storage
	err   error
	armed atomic.Bool
}

func (s *armedFailureStorage) GetWithContext(ctx context.Context, key string) ([]byte, error) {
	if s.armed.Load() {
		return nil, s.err
	}
	return s.Storage.GetWithContext(ctx, key)
}

// Test_Store_CachedIDReadErrorReachesCaller pins that a storage that fails when the store looks for
// the session under an ID it cached is reported, as it is for the ID the request names, and not
// taken for a session that is not there.
func Test_Store_CachedIDReadErrorReachesCaller(t *testing.T) {
	t.Parallel()
	errDown := errors.New("storage down")
	storage := &armedFailureStorage{Storage: memory.New(), err: errDown}
	store := NewStore(Config{Storage: storage})
	c := newRequest(t, fiber.New())

	require.NoError(t, mustGet(t, store, c).Save())

	storage.armed.Store(true)
	_, err := store.Get(c)
	require.ErrorIs(t, err, errDown)
}

// A store that is asked twice, for a visitor whose cookie names no session, looks that cookie up
// once. A store over the same cookie with a storage of its own still looks it up: what one storage
// does not hold, another may.
func Test_Store_Get_AskedAgainDoesNotLookUpTheNamedIDTwice(t *testing.T) {
	t.Parallel()
	app := fiber.New()
	storage := &keyLogStorage{Storage: memory.New()}
	store := NewStore(Config{Storage: storage})
	other := memory.New()
	holder := NewStore(Config{Storage: other, KeyGenerator: func() string { return "stale" }})
	require.Equal(t, "stale", seedSession(t, app, holder, func(s *Session) { s.Set("k", "v") }))
	sharing := NewStore(Config{Storage: other})

	c := newRequest(t, app)
	c.Request().Header.SetCookie("session_id", "stale")

	first, second := mustGet(t, store, c), mustGet(t, store, c)
	require.True(t, first.Fresh() && second.Fresh())
	require.NotEqual(t, first.ID(), second.ID(), "an ID that was not saved is not a session")

	lookups := 0
	for _, op := range storage.seen() {
		if op == "get:session:stale" {
			lookups++
		}
	}
	require.Equal(t, 1, lookups, "the cookie named no session the first time, and is not looked up again")

	third := mustGet(t, sharing, c)
	require.False(t, third.Fresh(), "the storage of another store holds the session the cookie names")
	require.Equal(t, "stale", third.ID())
}

// The PassLocalsToContext setting is read once for each app a store serves, and the store goes by
// the app, not by the last answer.
func Test_Store_PassesLocalsToContextByApp(t *testing.T) {
	t.Parallel()
	onCtx := newRequest(t, fiber.New(fiber.Config{PassLocalsToContext: true}))
	offCtx := newRequest(t, fiber.New())

	store := NewStore()
	for range 3 {
		require.True(t, store.passesLocalsToContext(onCtx))
		require.False(t, store.passesLocalsToContext(offCtx))
	}
}

// The ID the request named is kept as a copy: the application can rewrite the bytes of the cookie
// in place, and a store that is asked again must not take the new value for the one it has already
// looked up.
func Test_Store_Get_AskedAgainAfterTheCookieWasRewritten(t *testing.T) {
	t.Parallel()
	app := fiber.New()
	store := NewStore()
	live := seedSession(t, app, store, func(s *Session) { s.Set("k", "v") })
	stale := strings.Repeat("s", len(live)) // as long as the other, so that it is rewritten in place

	c := newRequest(t, app)
	c.Request().Header.SetCookie("session_id", stale)
	first := mustGet(t, store, c)
	require.True(t, first.Fresh(), "the cookie names no session")

	c.Request().Header.SetCookie("session_id", live)
	again := mustGet(t, store, c)
	require.False(t, again.Fresh(), "the store took the rewritten cookie for the ID it had looked up")
	require.Equal(t, live, again.ID())
}
