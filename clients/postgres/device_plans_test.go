package postgres

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDevicePlansIntegration(t *testing.T) {
	dsn := os.Getenv("AUTH_TEST_DATABASE_DSN")
	if dsn == "" {
		t.Skip("isolated test database required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "neuravpn_auth_test") {
		t.Fatal("refusing non-test database")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("slots_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := &Store{pool: pool}
	_, err = pool.Exec(ctx, `CREATE TABLE users(id TEXT PRIMARY KEY,autopay_enabled BOOLEAN DEFAULT TRUE,autopay_method_id TEXT DEFAULT 'saved',autopay_plan_id TEXT DEFAULT '90d',updated_at TIMESTAMPTZ);
 CREATE TABLE applied_payments(payment_id TEXT PRIMARY KEY,user_id TEXT,provider TEXT,plan_id TEXT,amount_value NUMERIC,currency TEXT);
 INSERT INTO users(id) VALUES('42'),('43');`)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.initDevicePlans(ctx); err != nil {
		t.Fatal(err)
	}
	p, err := s.GetDevicePlan("43")
	if err != nil || p.Enabled || p.ExtraSlots != 0 {
		t.Fatal("mass rollout not default off")
	}
	if _, err = s.CreateDeviceSlotOrder("43"); err == nil {
		t.Fatal("unlimited account charged")
	}
	pool.Exec(ctx, `INSERT INTO device_plans(user_id,enabled) VALUES('42',TRUE)`)
	var wg sync.WaitGroup
	ids := make(chan string, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o, e := s.CreateDeviceSlotOrder("42")
			if e != nil {
				t.Error(e)
			}
			ids <- o.ID
		}()
	}
	wg.Wait()
	close(ids)
	id := ""
	for got := range ids {
		if id != "" && got != id {
			t.Fatal("duplicate pending orders")
		}
		id = got
	}
	if err = s.SetDeviceOrderPayment(id, "pay1", "https://example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetDeviceSlotOrder(id, "43"); err == nil {
		t.Fatal("foreign order exposed")
	}
	if _, err = s.ApplyDeviceSlotOrder(id, "42", "forged"); err == nil {
		t.Fatal("wrong payment accepted")
	}
	var applied atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, e := s.ApplyDeviceSlotOrder(id, "42", "pay1")
			if e != nil {
				t.Error(e)
			}
			if ok {
				applied.Add(1)
			}
		}()
	}
	wg.Wait()
	p, err = s.GetDevicePlan("42")
	var enabled bool
	pool.QueryRow(ctx, `SELECT autopay_enabled FROM users WHERE id='42'`).Scan(&enabled)
	if err != nil || applied.Load() != 1 || p.ExtraSlots != 1 || enabled {
		t.Fatal("non-atomic fulfillment", p, applied.Load(), enabled)
	}
	if err = s.EnableAutopayWithDevices("42", 0); err == nil {
		t.Fatal("stale billing price accepted")
	}
	if err = s.EnableAutopayWithDevices("42", 1); err != nil {
		t.Fatal(err)
	}
	if err = s.RemoveExtraDeviceSlot("42", 2); err == nil {
		t.Fatal("stale removal accepted")
	}
	if err = s.RemoveExtraDeviceSlot("42", 1); err != nil {
		t.Fatal(err)
	}
	pool.QueryRow(ctx, `SELECT autopay_enabled FROM users WHERE id='42'`).Scan(&enabled)
	if enabled {
		t.Fatal("autopay not disabled on quantity change")
	}
	if err = s.RemoveExtraDeviceSlot("42", 0); err == nil {
		t.Fatal("negative slots")
	}
	p, _ = s.GetDevicePlan("43")
	if p.Enabled || p.ExtraSlots != 0 {
		t.Fatal("another account modified")
	}
}
