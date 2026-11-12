package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type Sale struct {
	Name    string
	Percent int
	EndsAt  time.Time
}

var saleName = regexp.MustCompile(`^[A-Z0-9][A-Z0-9_-]{0,47}$`)

func SaleAmount(amount float64, percent int) float64 {
	if percent < 1 || percent > 99 {
		return amount
	}
	return math.Max(1, math.Round(amount*(100-float64(percent)))/100)
}

func (s *Store) initSales(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS global_sale (
 singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
 name TEXT NOT NULL, percent INTEGER NOT NULL CHECK(percent BETWEEN 1 AND 99),
 ends_at TIMESTAMPTZ NOT NULL, created_by TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), stopped_at TIMESTAMPTZ
);`)
	return err
}

func (s *Store) GetActiveSale() (*Sale, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var sale Sale
	err := s.pool.QueryRow(ctx, `SELECT name,percent,ends_at FROM global_sale WHERE singleton AND stopped_at IS NULL AND ends_at>NOW()`).Scan(&sale.Name, &sale.Percent, &sale.EndsAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &sale, err
}

func (s *Store) CreateSale(sale Sale, adminID string) error {
	sale.Name = strings.ToUpper(strings.TrimSpace(sale.Name))
	if !saleName.MatchString(sale.Name) || sale.Percent < 1 || sale.Percent > 99 || !sale.EndsAt.After(time.Now()) {
		return errors.New("invalid sale")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The singleton row serializes concurrent admins and prevents silent replacement.
	tag, err := s.pool.Exec(ctx, `INSERT INTO global_sale(singleton,name,percent,ends_at,created_by) VALUES(TRUE,$1,$2,$3,$4)
 ON CONFLICT(singleton) DO UPDATE SET name=EXCLUDED.name,percent=EXCLUDED.percent,ends_at=EXCLUDED.ends_at,
 created_by=EXCLUDED.created_by,created_at=NOW(),stopped_at=NULL
 WHERE global_sale.stopped_at IS NOT NULL OR global_sale.ends_at<=NOW()`, sale.Name, sale.Percent, sale.EndsAt, adminID)
	if err == nil && tag.RowsAffected() != 1 {
		return fmt.Errorf("уже действует акция; сначала остановите её через /stop_sales НАЗВАНИЕ")
	}
	return err
}

func (s *Store) StopSale(name string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE global_sale SET stopped_at=NOW() WHERE singleton AND name=$1 AND stopped_at IS NULL AND ends_at>NOW()`, strings.ToUpper(strings.TrimSpace(name)))
	return tag.RowsAffected() == 1, err
}
