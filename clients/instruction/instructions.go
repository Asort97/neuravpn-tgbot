package instruct

import (
	"fmt"
	"log"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type InstructType int

const (
	Windows InstructType = iota
	Android
	IOS
)

type InstructionState struct {
	CurrentStep int
	MessageID   int
	ChatID      int64
}

var (
	windowsStates  = make(map[int64]*InstructionState)
	androidStates  = make(map[int64]*InstructionState)
	iosStates      = make(map[int64]*InstructionState)
	showCertButton = make(map[int64]bool) // Показывать ли кнопку "Получить сертификат"
)

// EnableCertButton включает отображение кнопки "Получить сертификат" для данного чата
func EnableCertButton(chatID int64, enable bool) {
	showCertButton[chatID] = enable
}

func SetInstructKeyboard(messageID int, chatID int64, instructType InstructType) {

	switch instructType {
	case Windows:
		windowsStates[chatID] = &InstructionState{
			CurrentStep: -1,
			MessageID:   messageID,
			ChatID:      chatID,
		}
	case Android:
		androidStates[chatID] = &InstructionState{
			CurrentStep: -1,
			MessageID:   messageID,
			ChatID:      chatID,
		}
	case IOS:
		iosStates[chatID] = &InstructionState{
			CurrentStep: -1,
			MessageID:   messageID,
			ChatID:      chatID,
		}
	}
}

func InstructionWindows(chatID int64, bot *tgbotapi.BotAPI, step int) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Windows/1.png", `Скачайте <a href="https://openvpn.net/community/">OpenVPN</a> с официального сайта`},
		{"InstructionPhotos/Windows/2.png", "После скачивания откройте трей в правом нижнем углу"},
		{"InstructionPhotos/Windows/3.png", "ПКМ по значку OpenVPN → Импорт → «Импорт файла конфигурации». Выберите файл, который мы вам отправим"},
		{"InstructionPhotos/Windows/4.png", "Снова ПКМ по значку и нажмите «Подключиться»"},
	}

	// Границы
	if step < 0 {
		step = 0
	}
	if step >= len(steps) {
		step = len(steps) - 1
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	var row []tgbotapi.InlineKeyboardButton

	if step > 0 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад", fmt.Sprintf("win_prev_%d", step)))
	}
	row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("Шаг %d/%d", step+1, len(steps)), "win_current"))
	if step < len(steps)-1 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("Вперёд ➡️", fmt.Sprintf("win_next_%d", step)))
	}
	rows = append(rows, row)

	if step == 0 {
		linkRow := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("Скачать ↗️", "https://swupdate.openvpn.org/community/releases/OpenVPN-2.6.15-I001-amd64.msi"),
		)

		rows = append(rows, linkRow)
	}

	// Добавляем кнопку сертификата если включена
	if showCertButton[chatID] {
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📥 Получить сертификат", "resend_access"),
		))
	}

	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("❌ Выйти", "nav_instructions"),
	))

	kb := tgbotapi.NewInlineKeyboardMarkup(rows...)

	// Если есть предыдущее сообщение — редактируем его медиа
	if state, exists := windowsStates[chatID]; exists && state.MessageID != 0 {

		image := tgbotapi.NewInputMediaPhoto(tgbotapi.FilePath(steps[step].photoPath))
		image.Caption = steps[step].caption
		image.ParseMode = "HTML"

		edit := tgbotapi.EditMessageMediaConfig{
			BaseEdit: tgbotapi.BaseEdit{
				ChatID:      chatID,
				MessageID:   state.MessageID,
				ReplyMarkup: &kb,
			},
			Media: image,
		}

		if _, err := bot.Send(edit); err != nil {
			log.Printf("edit media failed: %v", err)
			return
		}

		// Обновляем состояние
		state.CurrentStep = step
		windowsStates[chatID] = state
		return
	}

	// Иначе — первая отправка
	photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(steps[step].photoPath))
	photo.Caption = steps[step].caption
	photo.ParseMode = "HTML"
	photo.ReplyMarkup = kb

	msg, err := bot.Send(photo)
	if err != nil {
		log.Printf("send photo failed: %v", err)
		return
	}

	windowsStates[chatID] = &InstructionState{
		CurrentStep: step,
		MessageID:   msg.MessageID,
		ChatID:      chatID,
	}
}

func InstructionAndroid(chatID int64, bot *tgbotapi.BotAPI, step int) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Android/0.jpg", `Скачайте <a href="https://play.google.com/store/apps/details?id=net.openvpn.openvpn">OpenVPN</a> из Google Play`},
		{"InstructionPhotos/Android/1.jpg", "Откройте файловый менеджер и найдите файл сертификата"},
		{"InstructionPhotos/Android/2.jpg", "Нажмите на файл и выберите в меню OpenVPN"},
		{"InstructionPhotos/Android/3.jpg", "Нажмите OK и подключитесь"},
	}

	// Границы
	if step < 0 {
		step = 0
	}
	if step >= len(steps) {
		step = len(steps) - 1
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	var row []tgbotapi.InlineKeyboardButton

	if step > 0 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад", fmt.Sprintf("android_prev_%d", step)))
	}
	row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("Android %d/%d", step+1, len(steps)), "android_current"))
	if step < len(steps)-1 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("Вперёд ➡️", fmt.Sprintf("android_next_%d", step)))
	}

	rows = append(rows, row)

	if step == 0 {
		linkRow := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("Скачать ↗️", "https://play.google.com/store/apps/details?id=net.openvpn.openvpn"),
		)

		rows = append(rows, linkRow)
	}

	// Добавляем кнопку сертификата если включена
	if showCertButton[chatID] {
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📥 Получить сертификат", "resend_access"),
		))
	}

	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("❌ Выйти", "nav_instructions"),
	))

	kb := tgbotapi.NewInlineKeyboardMarkup(rows...)

	// Если есть сообщение — редактируем
	if state, ok := androidStates[chatID]; ok && state.MessageID != 0 {
		media := tgbotapi.NewInputMediaPhoto(tgbotapi.FilePath(steps[step].photoPath))
		media.Caption = steps[step].caption
		media.ParseMode = "HTML"

		edit := tgbotapi.EditMessageMediaConfig{
			BaseEdit: tgbotapi.BaseEdit{
				ChatID:      chatID,
				MessageID:   state.MessageID,
				ReplyMarkup: &kb,
			},
			Media: media,
		}
		if _, err := bot.Send(edit); err != nil {
			log.Printf("android edit media failed: %v", err)
			return
		}
		state.CurrentStep = step
		androidStates[chatID] = state
		return
	}

	// Первичная отправка
	if step == 0 {
		msg := tgbotapi.NewMessage(chatID, steps[step].caption)
		msg.ParseMode = "HTML"
		msg.ReplyMarkup = kb
		sent, err := bot.Send(msg)
		if err != nil {
			log.Printf("android send text failed: %v", err)
			return
		}
		androidStates[chatID] = &InstructionState{CurrentStep: step, MessageID: sent.MessageID, ChatID: chatID}
		return
	}

	photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(steps[step].photoPath))
	photo.Caption = steps[step].caption
	photo.ParseMode = "HTML"
	photo.ReplyMarkup = kb
	sent, err := bot.Send(photo)
	if err != nil {
		log.Printf("android send photo failed: %v", err)
		return
	}
	androidStates[chatID] = &InstructionState{CurrentStep: step, MessageID: sent.MessageID, ChatID: chatID}
}

func InstructionIos(chatID int64, bot *tgbotapi.BotAPI, step int) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Ios/0.png", `Скачайте <a href="https://apps.apple.com/kz/app/v2raytun/id6476628951">V2RayTun</a> из App Store`},
		{"InstructionPhotos/Ios/1.png", "Скопируйте ключ который вы получили (начинается на vless://...)"},
		{"InstructionPhotos/Ios/2.png", "Откройте V2RayTun и нажмите на + в правом верхнем углу"},
		{"InstructionPhotos/Ios/4.png", "Нажмите Импорт из буфера"},
		{"InstructionPhotos/Ios/5.png", "Далее нажмите кнопку подключиться и все работает."},
	}

	// Границы
	if step < 0 {
		step = 0
	}
	if step >= len(steps) {
		step = len(steps) - 1
	}

	var rows [][]tgbotapi.InlineKeyboardButton
	var row []tgbotapi.InlineKeyboardButton

	if step > 0 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("⬅️ Назад", fmt.Sprintf("ios_prev_%d", step)))
	}
	row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("iOS %d/%d", step+1, len(steps)), "ios_current"))
	if step < len(steps)-1 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("Вперёд ➡️", fmt.Sprintf("ios_next_%d", step)))
	}

	rows = append(rows, row)

	if step == 0 {
		linkRow := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("Скачать ↗️", "https://apps.apple.com/kz/app/v2raytun/id6476628951"),
		)
		rows = append(rows, linkRow)
	}

	// Добавляем кнопку сертификата если включена
	if showCertButton[chatID] {
		rows = append(rows, tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonData("📥 Получить сертификат", "resend_access"),
		))
	}

	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("❌ Выйти", "nav_instructions"),
	))

	kb := tgbotapi.NewInlineKeyboardMarkup(rows...)

	// Если есть сообщение — редактируем
	if state, ok := iosStates[chatID]; ok && state.MessageID != 0 {
		media := tgbotapi.NewInputMediaPhoto(tgbotapi.FilePath(steps[step].photoPath))
		media.Caption = steps[step].caption
		media.ParseMode = "HTML"

		edit := tgbotapi.EditMessageMediaConfig{
			BaseEdit: tgbotapi.BaseEdit{
				ChatID:      chatID,
				MessageID:   state.MessageID,
				ReplyMarkup: &kb,
			},
			Media: media,
		}
		if _, err := bot.Send(edit); err != nil {
			log.Printf("ios edit media failed: %v", err)
			return
		}

		state.CurrentStep = step
		iosStates[chatID] = state
		return
	}

	// Первичная отправка
	if step == 0 {
		msg := tgbotapi.NewMessage(chatID, steps[step].caption)
		msg.ParseMode = "HTML"
		msg.ReplyMarkup = kb
		sent, err := bot.Send(msg)
		if err != nil {
			log.Printf("ios send text failed: %v", err)
			return
		}
		iosStates[chatID] = &InstructionState{CurrentStep: step, MessageID: sent.MessageID, ChatID: chatID}
		return
	}

	photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(steps[step].photoPath))
	photo.Caption = steps[step].caption
	photo.ParseMode = "HTML"
	photo.ReplyMarkup = kb
	sent, err := bot.Send(photo)
	if err != nil {
		log.Printf("ios send photo failed: %v", err)
		return
	}
	iosStates[chatID] = &InstructionState{CurrentStep: step, MessageID: sent.MessageID, ChatID: chatID}
}

func ResetState(chatID int64) {
	delete(windowsStates, chatID)
	delete(androidStates, chatID)
	delete(iosStates, chatID)
}

// func SendInstructMenu(bot *tgbotapi.BotAPI, chatID int64) {
// 	buttons := tgbotapi.NewInlineKeyboardMarkup(
// 		tgbotapi.NewInlineKeyboardRow(
// 			tgbotapi.NewInlineKeyboardButtonData("🪟 Windows", "windows"),
// 			tgbotapi.NewInlineKeyboardButtonData("📱 Android", "android"),
// 		),
// 		tgbotapi.NewInlineKeyboardRow(
// 			tgbotapi.NewInlineKeyboardButtonData("🍎 IOS", "ios"),
// 		),
// 	)

// 	msg := tgbotapi.NewMessage(chatID, "Инструкция:")
// 	msg.ReplyMarkup = buttons
// 	bot.Send(msg)
// }
