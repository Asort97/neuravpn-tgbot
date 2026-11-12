package postgres

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSaleAmount(t *testing.T) {
	for _, tc := range []struct {
		base float64
		pct  int
		want float64
	}{{149, 50, 74.5}, {50, 50, 25}, {149, 33, 99.83}, {1, 99, 1}, {50, 99, 1}, {149, 0, 149}} {
		if got := SaleAmount(tc.base, tc.pct); got != tc.want {
			t.Fatal(tc, got)
		}
	}
}

func testSalesIntegration(t *testing.T, s *Store) {
	ctx := context.Background()
	defer s.pool.Exec(ctx, `DELETE FROM global_sale`)
	sale := Sale{Name: "BLACKFRIDAY", Percent: 50, EndsAt: time.Now().Add(time.Hour)}
	var wg sync.WaitGroup
	var count atomic.Int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.CreateSale(sale, "42") == nil {
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	if count.Load() != 1 {
		t.Fatal("overlapping sales accepted", count.Load())
	}
	got, err := s.GetActiveSale()
	if err != nil || got == nil || got.Percent != 50 {
		t.Fatal(got, err)
	}
	if ok, err := s.StopSale("OTHER"); err != nil || ok {
		t.Fatal("stopped wrong sale", err)
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO users(id) VALUES('sale-user');INSERT INTO device_plans(user_id,enabled) VALUES('sale-user',TRUE)`)
	if err != nil {
		t.Fatal(err)
	}
	order, err := s.CreateDeviceSlotOrder("sale-user")
	if err != nil || order.Amount != 25 {
		t.Fatal(order, err)
	}
	if ok, err := s.StopSale("BLACKFRIDAY"); err != nil || !ok {
		t.Fatal(ok, err)
	}
	retry, err := s.CreateDeviceSlotOrder("sale-user")
	if err != nil || retry.ID != order.ID || retry.Amount != 25 {
		t.Fatal("repriced retry", retry, err)
	}
	if _, err = s.ApplyDeviceSlotOrder(order.ID, "sale-user", "sale-pay"); err != nil {
		t.Fatal(err)
	}
	var paid float64
	if err = s.pool.QueryRow(ctx, `SELECT amount_value FROM applied_payments WHERE payment_id='yookassa:sale-pay'`).Scan(&paid); err != nil || paid != 25 {
		t.Fatal("wrong accounting", paid, err)
	}
	order, err = s.CreateDeviceSlotOrder("sale-user")
	if err != nil || order.Amount != 50 {
		t.Fatal("sale persisted after stop", order, err)
	}
	if err = s.CreateSale(sale, "42"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE global_sale SET ends_at=NOW()-INTERVAL '1 second'`); err != nil {
		t.Fatal(err)
	}
	if got, err = s.GetActiveSale(); err != nil || got != nil {
		t.Fatal("expired sale active", got, err)
	}
	if err = s.initSales(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err = s.GetActiveSale(); err != nil || got != nil {
		t.Fatal("restart revived sale", got, err)
	}
}
