package instruct

import (
	"fmt"
	"log"
	"strings"

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
	HasImage    bool
}

var (
	windowsStates = make(map[int64]*InstructionState)
	androidStates = make(map[int64]*InstructionState)
	iosStates     = make(map[int64]*InstructionState)
)

func SetInstructKeyboard(messageID int, chatID int64, instructType InstructType) {

	switch instructType {
	case Windows:
		windowsStates[chatID] = &InstructionState{
			CurrentStep: -1,
			MessageID:   messageID,
			ChatID:      chatID,
			HasImage:    false,
		}
	case Android:
		androidStates[chatID] = &InstructionState{
			CurrentStep: -1,
			MessageID:   messageID,
			ChatID:      chatID,
			HasImage:    false,
		}
	case IOS:
		iosStates[chatID] = &InstructionState{
			CurrentStep: -1,
			MessageID:   messageID,
			ChatID:      chatID,
			HasImage:    false,
		}
	}
}

func InstructionWindows(chatID int64, bot *tgbotapi.BotAPI, step int) (int, error) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"", `скачайте <a href="https://github.com/Mahdi-zarei/nekoray/releases/download/4.3.5/nekoray-4.3.5-2025-05-16-windows64.zip">nekoray</a>`},
		{"", "после завершения загрузки выполните следующие действия:\n1) найдите загруженный файл nekoray-windows64.zip.\n2) щелкните правой кнопкой мыши на файле и выберите \"извлечь все...\" или воспользуйтесь архиватором, например, winrar или 7-zip, чтобы распаковать содержимое в удобное для вас место на компьютере."},
		{"", "откройте папку с распакованными файлами. найдите файл nekobox.exe. дважды щелкните по нему, чтобы запустить программу."},
		{"InstructionPhotos/Windows/0.png", "в программе нажмите на кнопку \"сервера\" и далее \"добавить профиль из буфера обмена\" (предварительно вы должны скопировать ключ-подключения который мы вам отправили)"},
		{"InstructionPhotos/Windows/1.png", "активируйте режим tun и запустите конфигурацию, нажав по конфигу правой кнопкой мыши и выбрав опцию запуск работает!"},
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
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", fmt.Sprintf("win_prev_%d", step)))
	}
	row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "win_current"))
	if step < len(steps)-1 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("вперёд ➡️", fmt.Sprintf("win_next_%d", step)))
	}
	rows = append(rows, row)

	if step == 0 {
		linkRow := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("скачать ↗️", "https://github.com/Mahdi-zarei/nekoray/releases/download/4.3.5/nekoray-4.3.5-2025-05-16-windows64.zip"),
		)

		rows = append(rows, linkRow)
	}

	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("❌ выйти", "nav_instructions"),
	))

	kb := tgbotapi.NewInlineKeyboardMarkup(rows...)

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, exists := windowsStates[chatID]
	if !exists || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		switch {
		case needsImage && state.HasImage:
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
				log.Printf("windows edit media failed: %v", err)
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = true
				windowsStates[chatID] = state
				return state.MessageID, nil
			}
		case !needsImage && !state.HasImage:
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, state.MessageID, steps[step].caption, kb)
			edit.ParseMode = "HTML"
			if _, err := bot.Send(edit); err != nil {
				log.Printf("windows edit text failed: %v", err)
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = false
				windowsStates[chatID] = state
				return state.MessageID, nil
			}
		default:
			_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, state.MessageID))
			state.MessageID = 0
		}
	}

	var (
		msgID int
		err   error
	)
	if needsImage {
		msgID, err = sendInstructionPhoto(bot, chatID, steps[step].photoPath, steps[step].caption, kb)
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = sendInstructionText(bot, chatID, steps[step].caption, kb)
		if err != nil {
			return 0, err
		}
		state.HasImage = false
	}

	state.CurrentStep = step
	state.MessageID = msgID
	windowsStates[chatID] = state
	return msgID, nil
}

func InstructionAndroid(chatID int64, bot *tgbotapi.BotAPI, step int) (int, error) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Android/0.jpg", `скачайте <a href="https://play.google.com/store/apps/details?id=com.happproxy">happ - proxy utility</a> из Google Play`},
		{"InstructionPhotos/Android/1.jpg", "заходим в приложение и вставляем ключ из буфера обмена (предварительно вы должны скопировать ключ-подключения который мы вам отправили)"},
		{"InstructionPhotos/Android/2.jpg", "далее жмём на кнопку включения и vpn работает!"},
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
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", fmt.Sprintf("android_prev_%d", step)))
	}
	row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "android_current"))
	if step < len(steps)-1 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("вперёд ➡️", fmt.Sprintf("android_next_%d", step)))
	}

	rows = append(rows, row)

	if step == 0 {
		linkRow := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("скачать ↗️", "https://play.google.com/store/apps/details?id=com.happproxy"),
		)

		rows = append(rows, linkRow)
	}

	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("❌ выйти", "nav_instructions"),
	))

	kb := tgbotapi.NewInlineKeyboardMarkup(rows...)

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, ok := androidStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		switch {
		case needsImage && state.HasImage:
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
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = true
				androidStates[chatID] = state
				return state.MessageID, nil
			}
		case !needsImage && !state.HasImage:
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, state.MessageID, steps[step].caption, kb)
			edit.ParseMode = "HTML"
			if _, err := bot.Send(edit); err != nil {
				log.Printf("android edit text failed: %v", err)
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = false
				androidStates[chatID] = state
				return state.MessageID, nil
			}
		default:
			_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, state.MessageID))
			state.MessageID = 0
		}
	}

	var (
		msgID int
		err   error
	)
	if needsImage {
		msgID, err = sendInstructionPhoto(bot, chatID, steps[step].photoPath, steps[step].caption, kb)
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = sendInstructionText(bot, chatID, steps[step].caption, kb)
		if err != nil {
			return 0, err
		}
		state.HasImage = false
	}

	state.CurrentStep = step
	state.MessageID = msgID
	androidStates[chatID] = state
	return msgID, nil
}

func InstructionIos(chatID int64, bot *tgbotapi.BotAPI, step int) (int, error) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Ios/0.png", `скачайте <a href="https://apps.apple.com/kz/app/v2raytun/id6476628951">v2raytun</a> из app store`},
		{"InstructionPhotos/Ios/1.png", "скопируйте ключ, который получили (начинается на https://sub.static....)"},
		{"InstructionPhotos/Ios/2.png", "откройте V2RayTun и нажмите на + в правом верхнем углу"},
		{"InstructionPhotos/Ios/3.png", "выберите \"импорт из буфера\", подтвердите и нажмите \"подключиться\""},
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
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", fmt.Sprintf("ios_prev_%d", step)))
	}
	row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "ios_current"))
	if step < len(steps)-1 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("вперёд ➡️", fmt.Sprintf("ios_next_%d", step)))
	}

	rows = append(rows, row)

	if step == 0 {
		linkRow := tgbotapi.NewInlineKeyboardRow(
			tgbotapi.NewInlineKeyboardButtonURL("скачать ↗️", "https://apps.apple.com/kz/app/v2raytun/id6476628951"),
		)
		rows = append(rows, linkRow)
	}

	rows = append(rows, tgbotapi.NewInlineKeyboardRow(
		tgbotapi.NewInlineKeyboardButtonData("❌ выйти", "nav_instructions"),
	))

	kb := tgbotapi.NewInlineKeyboardMarkup(rows...)

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, ok := iosStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		switch {
		case needsImage && state.HasImage:
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
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = true
				iosStates[chatID] = state
				return state.MessageID, nil
			}
		case !needsImage && !state.HasImage:
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, state.MessageID, steps[step].caption, kb)
			edit.ParseMode = "HTML"
			if _, err := bot.Send(edit); err != nil {
				log.Printf("ios edit text failed: %v", err)
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = false
				iosStates[chatID] = state
				return state.MessageID, nil
			}
		default:
			_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, state.MessageID))
			state.MessageID = 0
		}
	}

	var (
		msgID int
		err   error
	)
	if needsImage {
		msgID, err = sendInstructionPhoto(bot, chatID, steps[step].photoPath, steps[step].caption, kb)
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = sendInstructionText(bot, chatID, steps[step].caption, kb)
		if err != nil {
			return 0, err
		}
		state.HasImage = false
	}

	state.CurrentStep = step
	state.MessageID = msgID
	iosStates[chatID] = state
	return msgID, nil
}

func sendInstructionPhoto(bot *tgbotapi.BotAPI, chatID int64, photoPath, caption string, kb tgbotapi.InlineKeyboardMarkup) (int, error) {
	photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(photoPath))
	photo.Caption = caption
	photo.ParseMode = "HTML"
	photo.ReplyMarkup = kb
	sent, err := bot.Send(photo)
	if err != nil {
		log.Printf("send photo failed: %v", err)
		return 0, err
	}
	return sent.MessageID, nil
}

func sendInstructionText(bot *tgbotapi.BotAPI, chatID int64, text string, kb tgbotapi.InlineKeyboardMarkup) (int, error) {
	msg := tgbotapi.NewMessage(chatID, text)
	msg.ParseMode = "HTML"
	msg.ReplyMarkup = kb
	sent, err := bot.Send(msg)
	if err != nil {
		log.Printf("send text failed: %v", err)
		return 0, err
	}
	return sent.MessageID, nil
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
