package sqlite

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	colorfulprint "github.com/Asort97/vpnBot/clients/colorfulPrint"
	"github.com/google/uuid"
)

type Store struct {
	path string
}

type UserData struct {
	Days                int64                         `json:"days"`
	CertRef             string                        `json:"certref"`
	LastDeduct          string                        `json:"last_deduct"`     // ISO8601 timestamp
	ReferredBy          string                        `json:"referred_by"`     // ID пользователя, который пригласил
	ReferralUsed        bool                          `json:"referral_used"`   // использовал ли свой реферальный бонус
	ReferralsCount      int                           `json:"referrals_count"` // сколько человек пригласил
	ReferralConfirmed   bool                          `json:"referral_confirmed"`
	ReferralConfirmedAt string                        `json:"referral_confirmed_at"`
	ReferrerRewardGiven bool                          `json:"referrer_reward_given"`
	Email               string                        `json:"email"`
	SubscriptionID      string                        `json:"subscription_id"`
	StartBonusClaimed   bool                          `json:"start_bonus_claimed"`
	StartBonusSource    string                        `json:"start_bonus_source"`
	StartBonusClaimedAt string                        `json:"start_bonus_claimed_at"`
	ConsentAt           string                        `json:"consent_at"` // ISO8601 timestamp, когда принял политику
	LinkToken           string                        `json:"link_token,omitempty"`
	LinkedTo            string                        `json:"linked_to,omitempty"`
	AppliedPayments     map[string]AppliedPaymentMeta `json:"applied_payments,omitempty"`
}

type AppliedPaymentMeta struct {
	Provider  string `json:"provider"`
	PlanID    string `json:"plan_id"`
	AppliedAt string `json:"applied_at"`
}

var (
	db   map[string]UserData
	dbMu sync.Mutex
)

func New(path string) *Store {
	return &Store{
		path: path,
	}
}

func (s *Store) loadUsersLocked() {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			// file doesn't exist yet — initialize empty DB
			db = make(map[string]UserData)
			return
		}
		// other read errors: keep db nil/empty
		return
	}

	if len(data) == 0 {
		db = make(map[string]UserData)
		return
	}

	var tmp map[string]UserData
	if err := json.Unmarshal(data, &tmp); err != nil {
		// invalid JSON — initialize empty DB (could also choose to preserve existing)
		db = make(map[string]UserData)
		return
	}
	db = tmp
}

func (s *Store) saveUsersLocked() error {
	data, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(s.path, data, 0644); err != nil {
		return err
	}

	return nil
}

func (s *Store) AddDays(userID string, days int64) error {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	now := time.Now().UTC()
	userData, exist := db[userID]

	if !exist {
		userData = UserData{
			Days:       days,
			LastDeduct: now.Format(time.RFC3339),
		}
	} else {
		prev := userData.Days
		userData.Days += days
		// если пополнение было с нуля -> начать новый 24ч цикл от момента пополнения
		if prev == 0 && userData.Days > 0 {
			userData.LastDeduct = now.Format(time.RFC3339)
		}
	}

	db[userID] = userData

	return s.saveUsersLocked()
}

func (s *Store) GetDays(userID string) (int64, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	userData, exist := db[userID]

	if exist {
		return userData.Days, nil
	} else {
		return 0, colorfulprint.PrintError(fmt.Sprintf("userid(%s) does not exist in DataBase", userID), nil)
	}
}

// SetDays overwrites day balance without running daily deductions (used when syncing with server expiry).
func (s *Store) SetDays(userID string, days int64) error {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	userData := db[userID]
	userData.Days = days
	if userData.LastDeduct == "" {
		userData.LastDeduct = time.Now().UTC().Format(time.RFC3339)
	}
	db[userID] = userData

	return s.saveUsersLocked()
}

func (s *Store) GetCertRef(userID string) (string, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	userData, exist := db[userID]

	if exist {
		return userData.CertRef, nil
	} else {
		return "", colorfulprint.PrintError(fmt.Sprintf("userid(%s) does not exist in DataBase", userID), nil)
	}
}

func (s *Store) ConsumeDays(userID string, days int64, nextCheck time.Time) (int64, error) {
	if days <= 0 {
		return 0, fmt.Errorf("days to consume must be positive")
	}

	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	userData, exist := db[userID]
	if !exist {
		return 0, fmt.Errorf("user %s not found", userID)
	}

	if userData.Days <= 0 {
		return userData.Days, nil
	}

	if days > userData.Days {
		days = userData.Days
	}

	userData.Days -= days
	if nextCheck.IsZero() {
		nextCheck = time.Now().UTC()
	} else {
		nextCheck = nextCheck.UTC()
	}
	userData.LastDeduct = nextCheck.Format(time.RFC3339)
	db[userID] = userData

	if err := s.saveUsersLocked(); err != nil {
		return 0, err
	}

	return userData.Days, nil
}

func (s *Store) GetAllUsers() map[string]UserData {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()
	result := make(map[string]UserData)
	for k, v := range db {
		result[k] = v
	}
	return result
}

// SetCertRef сохраняет или обновляет certRef для пользователя,
// не изменяя Days и корректно инициализируя запись при необходимости.
func (s *Store) SetCertRef(userID, certRef string) error {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	ud, ok := db[userID]
	if !ok {
		ud = UserData{
			Days:       0,
			LastDeduct: time.Now().UTC().Format(time.RFC3339),
		}
	}
	ud.CertRef = certRef
	db[userID] = ud
	return s.saveUsersLocked()
}

// SetEmail сохраняет email пользователя
func (s *Store) SetEmail(userID, email string) error {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	ud := db[userID]
	if ud.LastDeduct == "" {
		ud.LastDeduct = time.Now().UTC().Format(time.RFC3339)
	}
	ud.Email = email
	db[userID] = ud
	return s.saveUsersLocked()
}

// GetEmail возвращает email пользователя, если задан
func (s *Store) GetEmail(userID string) (string, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	ud, ok := db[userID]
	if !ok {
		return "", fmt.Errorf("user %s not found", userID)
	}
	return ud.Email, nil
}

// EnsureSubscriptionID returns existing subscription_id or creates UUIDv4 and stores it.
func (s *Store) EnsureSubscriptionID(userID string) (string, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	ud := db[userID]
	if strings.TrimSpace(ud.SubscriptionID) != "" {
		return ud.SubscriptionID, nil
	}
	ud.SubscriptionID = uuid.New().String()
	if ud.LastDeduct == "" {
		ud.LastDeduct = time.Now().UTC().Format(time.RFC3339)
	}
	db[userID] = ud
	if err := s.saveUsersLocked(); err != nil {
		return "", err
	}
	return ud.SubscriptionID, nil
}

// GetSubscriptionID returns subscription_id or empty string.
func (s *Store) GetSubscriptionID(userID string) (string, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	ud, ok := db[userID]
	if !ok {
		return "", fmt.Errorf("user %s not found", userID)
	}
	return ud.SubscriptionID, nil
}

// AcceptPrivacy помечает, что пользователь принял политику конфиденциальности
func (s *Store) AcceptPrivacy(userID string, at time.Time) error {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	ud := db[userID]
	if ud.LastDeduct == "" {
		ud.LastDeduct = time.Now().UTC().Format(time.RFC3339)
	}
	ud.ConsentAt = at.UTC().Format(time.RFC3339)
	db[userID] = ud
	return s.saveUsersLocked()
}

// IsNewUser проверяет, существует ли пользователь в базе данных
func (s *Store) IsNewUser(userID string) bool {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	_, exists := db[userID]
	return !exists
}

func (s *Store) IsStartBonusClaimed(userID string) (bool, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	ud, ok := db[userID]
	if !ok {
		return false, nil
	}
	return ud.StartBonusClaimed, nil
}

// ClaimStartBonus marks start bonus as claimed for a user. Returns true if claimed now, false if already claimed.
func (s *Store) ClaimStartBonus(userID string, source string, at time.Time) (bool, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	ud := db[userID]
	if ud.StartBonusClaimed {
		return false, nil
	}
	if ud.LastDeduct == "" {
		ud.LastDeduct = time.Now().UTC().Format(time.RFC3339)
	}
	ud.StartBonusClaimed = true
	ud.StartBonusSource = strings.TrimSpace(source)
	if at.IsZero() {
		at = time.Now()
	}
	ud.StartBonusClaimedAt = at.UTC().Format(time.RFC3339)
	db[userID] = ud

	return true, s.saveUsersLocked()
}

// RecordReferral записывает реферальную связь между новым пользователем и пригласившим
func (s *Store) RecordReferral(newUserID, referrerID string) error {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	// Проверяем, не использовал ли уже новый пользователь реферальный код
	if newUser, exists := db[newUserID]; exists && newUser.ReferredBy != "" {
		return fmt.Errorf("user %s already used referral code", newUserID)
	}

	// Обновляем нового пользователя
	newUser := db[newUserID]
	newUser.ReferredBy = referrerID
	newUser.ReferralUsed = true
	db[newUserID] = newUser

	return s.saveUsersLocked()
}

// ConfirmReferralAndRewardReferrer confirms referral subscription for new user and rewards referrer once.
// Returns referrerID and whether reward was granted in this call.
func (s *Store) ConfirmReferralAndRewardReferrer(newUserID string, rewardDays int64, at time.Time) (string, bool, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	newUser, exists := db[newUserID]
	if !exists {
		return "", false, nil
	}
	referrerID := strings.TrimSpace(newUser.ReferredBy)
	if referrerID == "" {
		return "", false, nil
	}

	changed := false
	if !newUser.ReferralConfirmed {
		if at.IsZero() {
			at = time.Now()
		}
		newUser.ReferralConfirmed = true
		newUser.ReferralConfirmedAt = at.UTC().Format(time.RFC3339)
		changed = true
	}

	if newUser.ReferrerRewardGiven {
		if changed {
			db[newUserID] = newUser
			if err := s.saveUsersLocked(); err != nil {
				return referrerID, false, err
			}
		}
		return referrerID, false, nil
	}

	referrer := db[referrerID]
	referrer.ReferralsCount++
	if rewardDays != 0 {
		referrer.Days += rewardDays
		if referrer.LastDeduct == "" {
			referrer.LastDeduct = time.Now().UTC().Format(time.RFC3339)
		}
	}
	db[referrerID] = referrer

	newUser.ReferrerRewardGiven = true
	db[newUserID] = newUser

	if err := s.saveUsersLocked(); err != nil {
		return referrerID, false, err
	}

	return referrerID, true, nil
}

// GetReferralsCount возвращает количество приглашенных пользователей
func (s *Store) GetReferralsCount(userID string) int {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	if userData, exist := db[userID]; exist {
		return userData.ReferralsCount
	}
	return 0
}

func (s *Store) IsPaymentApplied(userID, paymentID string) (bool, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	paymentID = strings.TrimSpace(paymentID)
	if paymentID == "" {
		return false, fmt.Errorf("paymentID is empty")
	}

	if ud, ok := db[userID]; ok && ud.AppliedPayments != nil {
		if _, exists := ud.AppliedPayments[paymentID]; exists {
			return true, nil
		}
	}

	for _, ud := range db {
		if ud.AppliedPayments == nil {
			continue
		}
		if _, exists := ud.AppliedPayments[paymentID]; exists {
			return true, nil
		}
	}

	return false, nil
}

func (s *Store) MarkPaymentApplied(userID, paymentID, provider, planID string, at time.Time) (bool, error) {
	dbMu.Lock()
	defer dbMu.Unlock()

	s.loadUsersLocked()

	userID = strings.TrimSpace(userID)
	paymentID = strings.TrimSpace(paymentID)
	provider = strings.TrimSpace(provider)
	planID = strings.TrimSpace(planID)

	if userID == "" {
		return false, fmt.Errorf("userID is empty")
	}
	if paymentID == "" {
		return false, fmt.Errorf("paymentID is empty")
	}
	if provider == "" {
		return false, fmt.Errorf("provider is empty")
	}
	if at.IsZero() {
		at = time.Now()
	}

	ud := db[userID]
	if ud.AppliedPayments == nil {
		ud.AppliedPayments = make(map[string]AppliedPaymentMeta)
	}
	if _, exists := ud.AppliedPayments[paymentID]; exists {
		return false, nil
	}

	ud.AppliedPayments[paymentID] = AppliedPaymentMeta{
		Provider:  provider,
		PlanID:    planID,
		AppliedAt: at.UTC().Format(time.RFC3339),
	}
	if ud.LastDeduct == "" {
		ud.LastDeduct = time.Now().UTC().Format(time.RFC3339)
	}
	db[userID] = ud

	if err := s.saveUsersLocked(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) SetLinkToken(userID, token string) error {
	dbMu.Lock()
	defer dbMu.Unlock()
	s.loadUsersLocked()
	u := db[userID]
	u.LinkToken = token
	db[userID] = u
	return s.saveUsersLocked()
}

func (s *Store) GetUserByLinkToken(token string) (string, error) {
	dbMu.Lock()
	defer dbMu.Unlock()
	s.loadUsersLocked()
	token = strings.TrimSpace(token)
	for id, u := range db {
		if u.LinkToken == token {
			return id, nil
		}
	}
	return "", fmt.Errorf("token not found")
}

func (s *Store) ClearLinkToken(userID string) error {
	dbMu.Lock()
	defer dbMu.Unlock()
	s.loadUsersLocked()
	u := db[userID]
	u.LinkToken = ""
	db[userID] = u
	return s.saveUsersLocked()
}

func (s *Store) SetLinkedTo(userID, linkedTo string) error {
	dbMu.Lock()
	defer dbMu.Unlock()
	s.loadUsersLocked()
	u := db[userID]
	u.LinkedTo = linkedTo
	db[userID] = u
	return s.saveUsersLocked()
}

func (s *Store) GetLinkedTo(userID string) (string, error) {
	dbMu.Lock()
	defer dbMu.Unlock()
	s.loadUsersLocked()
	u, ok := db[userID]
	if !ok {
		return "", nil
	}
	return u.LinkedTo, nil
}
