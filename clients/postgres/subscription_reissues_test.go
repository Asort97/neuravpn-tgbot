package postgres

import (
	"context"
	"testing"
)

func testSubscriptionReissueStore(t *testing.T, s *Store) {
	ctx := context.Background()
	if err := s.initSubscriptionReissues(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO users(id,subscription_id) VALUES('reissue-test','old-sub')`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, `INSERT INTO device_plans(user_id,enabled,extra_slots) VALUES('reissue-test',TRUE,2)`); err != nil {
		t.Fatal(err)
	}
	r, err := s.BeginSubscriptionReissue(ctx, "reissue-test", "", map[string]bool{"main": true, "offline": false})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := s.BeginSubscriptionReissue(ctx, "reissue-test", "", map[string]bool{"main": true, "offline": false})
	if err != nil || duplicate.Generation != r.Generation {
		t.Fatal("not idempotent", err)
	}
	if j, err := s.ClaimSubscriptionReissues(ctx, "offline"); err != nil || len(j) != 0 {
		t.Fatal("secondary ran before primary")
	}
	jobs, err := s.ClaimSubscriptionReissues(ctx, "main")
	if err != nil || len(jobs) != 1 {
		t.Fatal(err)
	}
	if err = s.SaveReissueBeforeState(ctx, jobs[0], []byte(`[]`)); err != nil {
		t.Fatal(err)
	}
	if err = s.CompleteSubscriptionReissue(ctx, jobs[0]); err != nil {
		t.Fatal(err)
	}
	var sub, method, plan string
	var auto bool
	var days, slots int
	err = s.pool.QueryRow(ctx, `SELECT subscription_id,autopay_method_id,autopay_plan_id,autopay_enabled,days,extra_slots FROM users JOIN device_plans ON user_id=id WHERE id='reissue-test'`).Scan(&sub, &method, &plan, &auto, &days, &slots)
	if err != nil || sub != r.SubID || method != "saved" || plan != "90d" || !auto || days != 123 || slots != 2 {
		t.Fatal("account metadata lost", err)
	}
	active, err := s.GetSubscriptionReissue(ctx, "reissue-test")
	if err != nil || !active.Active {
		t.Fatal(err)
	}
	ready, err := s.SubscriptionReissuePanelReady(ctx, "reissue-test", "main")
	if err != nil || !ready {
		t.Fatal(err)
	}
	ready, err = s.SubscriptionReissuePanelReady(ctx, "reissue-test", "offline")
	if err != nil || ready {
		t.Fatal("offline incorrectly complete")
	}
	pending, err := s.ClaimSubscriptionReissues(ctx, "offline")
	if err != nil || len(pending) != 1 {
		t.Fatal(err)
	}
	next, err := s.BeginSubscriptionReissue(ctx, "reissue-test", r.Generation, map[string]bool{"main": true, "offline": false})
	if err != nil || next.Generation == r.Generation {
		t.Fatal("offline blocked new primary generation", err)
	}
	if s.CompleteSubscriptionReissue(ctx, pending[0]) == nil {
		t.Fatal("stale generation acknowledged")
	}
	untouched, err := s.GetSubscriptionReissue(ctx, "43")
	if err != nil || untouched.Generation != "" {
		t.Fatal("other user changed")
	}
}
