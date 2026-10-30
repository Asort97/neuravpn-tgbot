package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const DeviceSlotPrice = 50
const MaxExtraDeviceSlots = 95

type DevicePlan struct {
	Enabled    bool `json:"enabled"`
	ExtraSlots int  `json:"extra_slots"`
}

type DeviceSlotOrder struct {
	ID        string `json:"id"`
	UserID    string `json:"-"`
	PaymentID string `json:"-"`
	URL       string `json:"confirmation_url,omitempty"`
	Applied   bool   `json:"applied"`
}

func (s *Store) initDevicePlans(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
CREATE TABLE IF NOT EXISTS device_plans (
 user_id TEXT PRIMARY KEY REFERENCES users(id),
 enabled BOOLEAN NOT NULL DEFAULT FALSE,
 extra_slots INTEGER NOT NULL DEFAULT 0 CHECK(extra_slots BETWEEN 0 AND 95),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS device_slot_orders (
 id TEXT PRIMARY KEY,
 user_id TEXT NOT NULL REFERENCES users(id),
 payment_id TEXT UNIQUE,
 confirmation_url TEXT NOT NULL DEFAULT '',
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 applied_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS device_slot_orders_user ON device_slot_orders(user_id,created_at);
`)
	return err
}

func (s *Store) GetDevicePlan(userID string) (DevicePlan, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var p DevicePlan
	err := s.pool.QueryRow(ctx, `SELECT enabled,extra_slots FROM device_plans WHERE user_id=$1`, userID).Scan(&p.Enabled, &p.ExtraSlots)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	return p, err
}

// Orders are persisted before contacting the payment provider. Retrying an
// uncertain network result reuses the same provider idempotency key.
func (s *Store) CreateDeviceSlotOrder(userID string) (DeviceSlotOrder, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return DeviceSlotOrder{}, err
	}
	defer tx.Rollback(ctx)
	var enabled bool
	var extra int
	if err = tx.QueryRow(ctx, `SELECT enabled,extra_slots FROM device_plans WHERE user_id=$1 FOR UPDATE`, userID).Scan(&enabled, &extra); err != nil {
		return DeviceSlotOrder{}, err
	}
	if !enabled || extra >= MaxExtraDeviceSlots {
		return DeviceSlotOrder{}, errors.New("purchase unavailable")
	}
	o := DeviceSlotOrder{UserID: userID}
	err = tx.QueryRow(ctx, `SELECT id,COALESCE(payment_id,''),confirmation_url FROM device_slot_orders WHERE user_id=$1 AND applied_at IS NULL AND created_at>NOW()-INTERVAL '20 minutes' ORDER BY created_at DESC LIMIT 1`, userID).Scan(&o.ID, &o.PaymentID, &o.URL)
	if errors.Is(err, pgx.ErrNoRows) {
		o.ID = uuid.NewString()
		_, err = tx.Exec(ctx, `INSERT INTO device_slot_orders(id,user_id) VALUES($1,$2)`, o.ID, userID)
	}
	if err != nil {
		return o, err
	}
	return o, tx.Commit(ctx)
}

func (s *Store) SetDeviceOrderPayment(orderID, paymentID, paymentURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE device_slot_orders SET payment_id=$2,confirmation_url=$3 WHERE id=$1 AND (payment_id IS NULL OR payment_id=$2)`, orderID, paymentID, paymentURL)
	if err == nil && tag.RowsAffected() != 1 {
		return errors.New("device order not found")
	}
	return err
}

func (s *Store) GetDeviceSlotOrder(orderID, userID string) (DeviceSlotOrder, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o := DeviceSlotOrder{ID: orderID, UserID: userID}
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(payment_id,''),confirmation_url,applied_at IS NOT NULL FROM device_slot_orders WHERE id=$1 AND user_id=$2`, orderID, userID).Scan(&o.PaymentID, &o.URL, &o.Applied)
	return o, err
}

func (s *Store) ApplyDeviceSlotOrder(orderID, userID, paymentID string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	// Consistent lock order with order creation and quantity changes.
	var extra int
	if err = tx.QueryRow(ctx, `SELECT extra_slots FROM device_plans WHERE user_id=$1 FOR UPDATE`, userID).Scan(&extra); err != nil {
		return false, err
	}
	var savedID string
	var applied bool
	err = tx.QueryRow(ctx, `SELECT COALESCE(payment_id,''),applied_at IS NOT NULL FROM device_slot_orders WHERE id=$1 AND user_id=$2 FOR UPDATE`, orderID, userID).Scan(&savedID, &applied)
	if err != nil {
		return false, err
	}
	if savedID != "" && savedID != paymentID {
		return false, errors.New("payment identity mismatch")
	}
	if applied {
		return false, nil
	}
	if extra >= MaxExtraDeviceSlots {
		return false, errors.New("device slots at maximum; payment needs review")
	}
	_, err = tx.Exec(ctx, `UPDATE device_plans SET extra_slots=extra_slots+1,updated_at=NOW() WHERE user_id=$1`, userID)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE users SET autopay_enabled=FALSE,updated_at=NOW() WHERE id=$1`, userID)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE device_slot_orders SET payment_id=$2,applied_at=NOW() WHERE id=$1`, orderID, paymentID)
	if err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO applied_payments(payment_id,user_id,provider,plan_id,amount_value,currency) VALUES($1,$2,'yookassa','device_slot',50,'RUB')`, "yookassa:"+paymentID, userID)
	if err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *Store) RemoveExtraDeviceSlot(userID string, expected int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `UPDATE device_plans SET extra_slots=extra_slots-1,updated_at=NOW() WHERE user_id=$1 AND enabled AND extra_slots=$2 AND extra_slots>0`, userID, expected)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("device plan changed")
	}
	_, err = tx.Exec(ctx, `UPDATE users SET autopay_enabled=FALSE,updated_at=NOW() WHERE id=$1`, userID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) GetEnabledDevicePlans() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT user_id FROM device_plans WHERE enabled`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) GetPendingDeviceSlotOrders() ([]DeviceSlotOrder, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := s.pool.Query(ctx, `SELECT id,user_id,payment_id FROM device_slot_orders WHERE applied_at IS NULL AND payment_id IS NOT NULL AND created_at>NOW()-INTERVAL '2 days' ORDER BY created_at LIMIT 50`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var orders []DeviceSlotOrder
	for rows.Next() {
		var o DeviceSlotOrder
		if err = rows.Scan(&o.ID, &o.UserID, &o.PaymentID); err != nil {
			return nil, err
		}
		orders = append(orders, o)
	}
	return orders, rows.Err()
}

// Lock the device plan before the user, just like slot purchases do. A stale
// confirmation must not enable billing at a newly changed device price.
func (s *Store) EnableAutopayWithDevices(userID string, expectedExtra int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var extra int
	err = tx.QueryRow(ctx, `SELECT extra_slots FROM device_plans WHERE user_id=$1 FOR UPDATE`, userID).Scan(&extra)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if extra != expectedExtra {
		return errors.New("device price changed")
	}
	tag, err := tx.Exec(ctx, `UPDATE users SET autopay_enabled=TRUE,updated_at=NOW() WHERE id=$1 AND COALESCE(autopay_method_id,'')<>'' AND COALESCE(autopay_plan_id,'')<>''`, userID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("saved card unavailable")
	}
	return tx.Commit(ctx)
}
