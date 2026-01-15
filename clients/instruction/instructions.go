package instruct

import (
	"fmt"
	"html"
	"log"
	"strings"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type InstructType int

const (
	Windows InstructType = iota
	Android
	IOS
	MacOS
)

type InstructionState struct {
	CurrentStep int
	MessageID   int
	ChatID      int64
	HasImage    bool
}

var (
	windowsStates   = make(map[int64]*InstructionState)
	androidStates   = make(map[int64]*InstructionState)
	iosStates       = make(map[int64]*InstructionState)
	macosStates     = make(map[int64]*InstructionState)
	instructionKeys = make(map[int64]string)
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
	case MacOS:
		macosStates[chatID] = &InstructionState{
			CurrentStep: -1,
			MessageID:   messageID,
			ChatID:      chatID,
			HasImage:    false,
		}
	}
}

// SetInstructionKey stores a per-chat key used in captions.
func SetInstructionKey(chatID int64, key string) {
	instructionKeys[chatID] = key
}

func InstructionWindows(chatID int64, bot *tgbotapi.BotAPI, step int) (int, error) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"", `скачайте <a href="https://github.com/Asort97/neuravpn-client/releases/tag/v1.0.0">neuravpn client</a>`},
		{"", "после завершения загрузки выполните следующие действия:\n1) найдите загруженный файл release1.0.0.zip.\n2) щелкните правой кнопкой мыши на файле и выберите «извлечь все...» или воспользуйтесь архиватором, например, winrar или 7-zip, чтобы распаковать содержимое в удобное для вас место на компьютере."},
		{"", "откройте папку с распакованными файлами. найдите файл neuravpn.exe. щелкните по нему правой кнопкой мыши и запустите от имени администратора."},
		{"InstructionPhotos/Windows/0.mp4", "предварительно скопировав ключ доступа, в программе нажмите на кнопку «вставить из буфера»"},
		{"InstructionPhotos/Windows/1.mp4", "подключитесь к vpn, нажав по большой кнопке в центру. вы подключены!"},
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

	caption := steps[step].caption
	if step == 3 {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n<code>%s</code>\n(нажмите чтобы копировать)", caption, html.EscapeString(key))
		}
	}

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, exists := windowsStates[chatID]
	if !exists || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		switch {
		case needsImage && state.HasImage:
			var media interface{}
			if isAnimationPath(steps[step].photoPath) {
				animation := tgbotapi.NewInputMediaVideo(tgbotapi.FilePath(steps[step].photoPath))
				animation.Type = "animation"
				animation.Caption = caption
				animation.ParseMode = "HTML"
				media = animation
			} else {
				image := tgbotapi.NewInputMediaPhoto(tgbotapi.FilePath(steps[step].photoPath))
				image.Caption = caption
				image.ParseMode = "HTML"
				media = image
			}

			edit := tgbotapi.EditMessageMediaConfig{
				BaseEdit: tgbotapi.BaseEdit{
					ChatID:      chatID,
					MessageID:   state.MessageID,
					ReplyMarkup: &kb,
				},
				Media: media,
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
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, state.MessageID, caption, kb)
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
		if isAnimationPath(steps[step].photoPath) {
			msgID, err = sendInstructionAnimation(bot, chatID, steps[step].photoPath, caption, kb)
		} else {
			msgID, err = sendInstructionPhoto(bot, chatID, steps[step].photoPath, caption, kb)
		}
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = sendInstructionText(bot, chatID, caption, kb)
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
		{"InstructionPhotos/Android/0.MP4", `скачайте <a href="https://play.google.com/store/apps/details?id=com.v2raytun.android&hl=ru">v2raytun</a> из google play`},
		{"InstructionPhotos/Android/1.MP4", "заходим в приложение и вставляем ключ из буфера обмена. предварительно вы должны скопировать ключ-подключения который мы вам отправили"},
		{"InstructionPhotos/Android/2.MP4", "далее жмём на кнопку включения и vpn работает!"},
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

	caption := steps[step].caption
	if step == 1 {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n<code>%s</code>\n(нажмите чтобы копировать)", caption, html.EscapeString(key))
		}
	}

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, ok := androidStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		switch {
		case needsImage && state.HasImage:
			media := tgbotapi.NewInputMediaVideo(tgbotapi.FilePath(steps[step].photoPath))
			media.Type = "animation"
			media.Caption = caption
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
				return state.MessageID, err
			} else {
				state.CurrentStep = step
				state.HasImage = true
				androidStates[chatID] = state
				return state.MessageID, nil
			}
		case !needsImage && !state.HasImage:
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, state.MessageID, caption, kb)
			edit.ParseMode = "HTML"
			if _, err := bot.Send(edit); err != nil {
				log.Printf("android edit text failed: %v", err)
				return state.MessageID, err
			} else {
				state.CurrentStep = step
				state.HasImage = false
				androidStates[chatID] = state
				return state.MessageID, nil
			}
		default:
			log.Printf("android edit skipped due to content type mismatch")
			return state.MessageID, fmt.Errorf("android instruction content type mismatch")
		}
	}

	var (
		msgID int
		err   error
	)
	if needsImage {
		msgID, err = sendInstructionAnimation(bot, chatID, steps[step].photoPath, caption, kb)
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = sendInstructionText(bot, chatID, caption, kb)
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
		{"InstructionPhotos/Ios/0.MP4", `скачайте <a href="https://apps.apple.com/kz/app/v2raytun/id6476628951">v2raytun</a> из app store`},
		{"InstructionPhotos/Ios/1.MP4", "заходим в приложение и вставляем ключ из буфера обмена. предварительно вы должны скопировать ключ-подключения который мы вам отправили"},
		{"InstructionPhotos/Ios/2.MP4", "далее жмём на кнопку включения и vpn работает!"},
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

	caption := steps[step].caption
	if step == 1 {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n<code>%s</code>\n(нажмите чтобы копировать)", caption, html.EscapeString(key))
		}
	}

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, ok := iosStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		switch {
		case needsImage && state.HasImage:
			var media interface{}
			if isAnimationPath(steps[step].photoPath) {
				animation := tgbotapi.NewInputMediaVideo(tgbotapi.FilePath(steps[step].photoPath))
				animation.Type = "animation"
				animation.Caption = caption
				animation.ParseMode = "HTML"
				media = animation
			} else {
				photo := tgbotapi.NewInputMediaPhoto(tgbotapi.FilePath(steps[step].photoPath))
				photo.Caption = caption
				photo.ParseMode = "HTML"
				media = photo
			}

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
				return state.MessageID, err
			} else {
				state.CurrentStep = step
				state.HasImage = true
				iosStates[chatID] = state
				return state.MessageID, nil
			}
		case !needsImage && !state.HasImage:
			edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, state.MessageID, caption, kb)
			edit.ParseMode = "HTML"
			if _, err := bot.Send(edit); err != nil {
				log.Printf("ios edit text failed: %v", err)
				return state.MessageID, err
			} else {
				state.CurrentStep = step
				state.HasImage = false
				iosStates[chatID] = state
				return state.MessageID, nil
			}
		default:
			log.Printf("ios edit skipped due to content type mismatch")
			return state.MessageID, fmt.Errorf("ios instruction content type mismatch")
		}
	}

	var (
		msgID int
		err   error
	)
	if needsImage {
		if isAnimationPath(steps[step].photoPath) {
			msgID, err = sendInstructionAnimation(bot, chatID, steps[step].photoPath, caption, kb)
		} else {
			msgID, err = sendInstructionPhoto(bot, chatID, steps[step].photoPath, caption, kb)
		}
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = sendInstructionText(bot, chatID, caption, kb)
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

func InstructionMacOS(chatID int64, bot *tgbotapi.BotAPI, step int) (int, error) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"", `скачайте <a href="https://apps.apple.com/kz/app/v2raytun/id6476628951">v2raytun</a> из app store`},
		{"", "заходим в приложение и вставляем ключ из буфера обмена. предварительно вы должны скопировать ключ-подключения который мы вам отправили"},
		{"", "далее жмём на кнопку включения и vpn работает!"},
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
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("⬅️ назад", fmt.Sprintf("macos_prev_%d", step)))
	}
	row = append(row, tgbotapi.NewInlineKeyboardButtonData(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "macos_current"))
	if step < len(steps)-1 {
		row = append(row, tgbotapi.NewInlineKeyboardButtonData("вперёд ➡️", fmt.Sprintf("macos_next_%d", step)))
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

	caption := steps[step].caption
	if step == 1 {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n<code>%s</code>\n(нажмите чтобы копировать)", caption, html.EscapeString(key))
		}
	}

	state, ok := macosStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		edit := tgbotapi.NewEditMessageTextAndMarkup(chatID, state.MessageID, caption, kb)
		edit.ParseMode = "HTML"
		if _, err := bot.Send(edit); err != nil {
			log.Printf("macos edit text failed: %v", err)
			return state.MessageID, err
		}
		state.CurrentStep = step
		state.HasImage = false
		macosStates[chatID] = state
		return state.MessageID, nil
	}

	msgID, err := sendInstructionText(bot, chatID, caption, kb)
	if err != nil {
		return 0, err
	}
	state.CurrentStep = step
	state.MessageID = msgID
	state.HasImage = false
	macosStates[chatID] = state
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

func sendInstructionAnimation(bot *tgbotapi.BotAPI, chatID int64, animationPath, caption string, kb tgbotapi.InlineKeyboardMarkup) (int, error) {
	animation := tgbotapi.NewAnimation(chatID, tgbotapi.FilePath(animationPath))
	animation.Caption = caption
	animation.ParseMode = "HTML"
	animation.ReplyMarkup = kb
	sent, err := bot.Send(animation)
	if err != nil {
		log.Printf("send animation failed: %v", err)
		return 0, err
	}
	return sent.MessageID, nil
}

func isAnimationPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".mp4")
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
	delete(macosStates, chatID)
	delete(instructionKeys, chatID)
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
