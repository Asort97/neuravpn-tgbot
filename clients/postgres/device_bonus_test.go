package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func testDeviceBonus(t *testing.T, s *Store) {
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `INSERT INTO users(id) VALUES('71'),('72'); INSERT INTO device_plans(user_id,enabled,extra_slots,synced_extra_slots,last_synced_at) VALUES('71',TRUE,2,2,NOW())`)
	if err != nil {
		t.Fatal(err)
	}
	applied, err := s.GrantDeviceBonus(ctx, "message-1", "42", []string{"72", "71"}, 3)
	if err != nil || !applied {
		t.Fatal(applied, err)
	}
	for _, id := range []string{"71", "72"} {
		p, err := s.GetDevicePlan(id)
		if err != nil || p.BonusSlots != 3 || (id == "71" && (p.ExtraSlots != 2 || p.Limit() != 10)) || (id == "72" && p.Enabled) {
			t.Fatal(id, p, err)
		}
		var days int
		var auto bool
		if err := s.pool.QueryRow(ctx, `SELECT days,autopay_enabled FROM users WHERE id=$1`, id).Scan(&days, &auto); err != nil || days != 123 || !auto {
			t.Fatal("grant changed billing/days", err)
		}
	}
	if ok, err := s.GrantDeviceBonus(ctx, "message-1", "42", []string{"71", "72"}, 3); err != nil || ok {
		t.Fatal("replay charged twice", err)
	}
	if _, err := s.GrantDeviceBonus(ctx, "message-1", "43", []string{"71", "72"}, 3); err == nil {
		t.Fatal("changed actor accepted")
	}
	if err := s.MarkDevicePlanSynced("71", 2, 0); err != nil {
		t.Fatal(err)
	}
	var synced int
	if err := s.pool.QueryRow(ctx, `SELECT synced_bonus_slots FROM device_plans WHERE user_id='71'`).Scan(&synced); err != nil || synced != 0 {
		t.Fatal("stale sync acknowledged", err)
	}
	ids, err := s.GetEnabledDevicePlans()
	found := false
	for _, id := range ids {
		if id == "71" {
			found = true
		}
	}
	if err != nil || !found {
		t.Fatal("bonus change not queued", err)
	}
	if err := s.MarkDevicePlanSynced("71", 2, 3); err != nil {
		t.Fatal(err)
	}
	var count atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.GrantDeviceBonus(ctx, "message-2", "42", []string{"71", "72"}, 2)
			if err != nil {
				t.Error(err)
			}
			if ok {
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatal("concurrent duplicate applied", count.Load())
	}
	for _, tc := range []struct {
		ids []string
		n   int
	}{{[]string{"71", "72"}, 95}, {[]string{"71", "999999"}, 1}, {[]string{"71", "71"}, 1}, {[]string{"71"}, 0}} {
		if _, err := s.GrantDeviceBonus(ctx, "bad", "42", tc.ids, tc.n); err == nil {
			t.Fatal("invalid batch committed", tc)
		}
	}
	if err := s.initDevicePlans(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := s.GetDevicePlan("71")
	if err != nil || p.BonusSlots != 5 || p.ExtraSlots != 2 {
		t.Fatal("rollback/restart changed plan", p, err)
	}
	order, err := s.CreateDeviceSlotOrder("71")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyDeviceSlotOrder(order.ID, "71", "bonus-test-payment"); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveExtraDeviceSlot("71", 3); err != nil {
		t.Fatal(err)
	}
	p, _ = s.GetDevicePlan("71")
	if p.BonusSlots != 5 || p.ExtraSlots != 2 {
		t.Fatal("paid slot mutation consumed bonus", p)
	}
}
