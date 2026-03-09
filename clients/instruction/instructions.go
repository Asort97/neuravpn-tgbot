package instruct

import (
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log"
	"net/url"
	"strings"
	"sync"

	"github.com/Asort97/vpnBot/clients/rawkbd"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type InstructType int

const (
	Windows InstructType = iota
	Android
	IOS
	MacOS
)

const (
	instructionIconBackID     = "5264852846527941278"
	instructionIconNextID     = "5262783080313164902"
	instructionIconDownloadID = "5264948349420739524"
	instructionIconExitID     = "5264852846527941278"
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

	instructionEmojiCheckOnce sync.Once
	instructionEmojiCheckMu   sync.RWMutex
	instructionEmojiValidity  = make(map[string]bool)
)

type instructionEmojiSpec struct {
	name     string
	id       string
	fallback string
}

var instructionEmojiSpecs = []instructionEmojiSpec{
	{name: "back", id: instructionIconBackID, fallback: "⬅️"},
	{name: "next", id: instructionIconNextID, fallback: "➡️"},
	{name: "download", id: instructionIconDownloadID, fallback: "⬇️"},
	{name: "exit", id: instructionIconExitID, fallback: "✖️"},
}

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

// ValidateCustomEmojiIDs checks instruction custom emoji IDs once and logs warnings for unavailable IDs.
func ValidateCustomEmojiIDs(bot *tgbotapi.BotAPI) {
	validateInstructionEmojiIDs(bot)
}

func validateInstructionEmojiIDs(bot *tgbotapi.BotAPI) {
	instructionEmojiCheckOnce.Do(func() {
		if bot == nil {
			log.Printf("instruction custom emoji validation skipped: bot is nil")
			return
		}

		uniq := make(map[string]struct{})
		var ids []string
		for _, spec := range instructionEmojiSpecs {
			id := strings.TrimSpace(spec.id)
			if id == "" {
				continue
			}
			if _, exists := uniq[id]; exists {
				continue
			}
			uniq[id] = struct{}{}
			ids = append(ids, id)
		}

		if len(ids) == 0 {
			return
		}

		params := tgbotapi.Params{}
		if err := params.AddInterface("custom_emoji_ids", ids); err != nil {
			log.Printf("instruction custom emoji validation build request failed: %v", err)
			return
		}

		resp, err := bot.MakeRequest("getCustomEmojiStickers", params)
		if err != nil {
			log.Printf("instruction custom emoji validation request failed: %v", err)
			return
		}
		if !resp.Ok {
			log.Printf("instruction custom emoji validation telegram error %d: %s", resp.ErrorCode, resp.Description)
			return
		}

		var stickers []struct {
			CustomEmojiID string `json:"custom_emoji_id"`
		}
		if err := json.Unmarshal(resp.Result, &stickers); err != nil {
			log.Printf("instruction custom emoji validation parse failed: %v", err)
			return
		}

		found := make(map[string]bool, len(stickers))
		for _, sticker := range stickers {
			id := strings.TrimSpace(sticker.CustomEmojiID)
			if id != "" {
				found[id] = true
			}
		}

		instructionEmojiCheckMu.Lock()
		for _, id := range ids {
			instructionEmojiValidity[id] = found[id]
		}
		instructionEmojiCheckMu.Unlock()

		for _, spec := range instructionEmojiSpecs {
			if !found[spec.id] {
				log.Printf("warning: instruction custom emoji id '%s' (%s) is unavailable; fallback text emoji will be used", spec.id, spec.name)
			}
		}
	})
}

func resolveInstructionIconID(bot *tgbotapi.BotAPI, iconID string) string {
	iconID = strings.TrimSpace(iconID)
	if iconID == "" {
		return ""
	}

	validateInstructionEmojiIDs(bot)

	instructionEmojiCheckMu.RLock()
	valid, known := instructionEmojiValidity[iconID]
	instructionEmojiCheckMu.RUnlock()
	if known && valid {
		return iconID
	}

	return ""
}

func instructionCallbackButton(bot *tgbotapi.BotAPI, text, callbackData, iconID, fallbackEmoji string) rawkbd.Button {
	resolvedID := resolveInstructionIconID(bot, iconID)
	if resolvedID == "" && fallbackEmoji != "" {
		text = fallbackEmoji + " " + text
	}
	return rawkbd.CallbackButton(text, callbackData, "", resolvedID)
}

func instructionURLButton(bot *tgbotapi.BotAPI, text, url, iconID, fallbackEmoji string) rawkbd.Button {
	resolvedID := resolveInstructionIconID(bot, iconID)
	if resolvedID == "" && fallbackEmoji != "" {
		text = fallbackEmoji + " " + text
	}
	return rawkbd.URLButton(text, url, resolvedID)
}

func InstructionWindows(chatID int64, bot *tgbotapi.BotAPI, step int) (int, error) {
	downloadURL := "https://asort97.github.io/neuravpn-site"
	keyStep := 3

	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Windows/neuravpn_app/0.png", `скачайте последнюю версию neuravpn c <a href="https://asort97.github.io/neuravpn-site/">сайта</a>, нажав кнопку "Скачать для Windows"`},
		{"", "после завершения загрузки выполните следующие действия:\n1) найдите загруженный файл neuravpn_windows_vX.X.X.zip.\n2) щелкните правой кнопкой мыши на файле и выберите «извлечь все...» или воспользуйтесь архиватором, например, winrar или 7-zip, чтобы распаковать содержимое в удобное для вас место на компьютере."},
		{"", "откройте папку с распакованными файлами. найдите файл neuravpn.exe. щелкните по нему правой кнопкой мыши и запустите от имени администратора."},
		{"InstructionPhotos/Windows/0.mp4", "предварительно скопировав ключ доступа, в программе нажмите на кнопку «вставить из буфера» или просто нажмите на кнопку ниже «авто-подключение»"},
		{"InstructionPhotos/Windows/1.mp4", "подключитесь к vpn, нажав по большой кнопке в центру."},
	}

	// Границы
	if step < 0 {
		step = 0
	}
	if step >= len(steps) {
		step = len(steps) - 1
	}

	var rows [][]rawkbd.Button
	var row []rawkbd.Button

	if step > 0 {
		row = append(row, instructionCallbackButton(bot, "назад", fmt.Sprintf("win_prev_%d", step), instructionIconBackID, "⬅️"))
	}
	row = append(row, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "win_current", "", ""))
	if step < len(steps)-1 {
		row = append(row, instructionCallbackButton(bot, "вперёд", fmt.Sprintf("win_next_%d", step), instructionIconNextID, "➡️"))
	}
	rows = append(rows, row)

	if step == 0 && strings.TrimSpace(downloadURL) != "" {
		linkRow := []rawkbd.Button{
			instructionURLButton(
				bot,
				"скачать",
				downloadURL,
				instructionIconDownloadID,
				"⬇️",
			),
		}
		rows = append(rows, linkRow)
	}

	if step == keyStep {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			autoURL := "https://asort97.github.io/neuravpn-site/?open=1&auto=1&v=" + url.QueryEscape(key)
			rows = append(rows, []rawkbd.Button{
				instructionURLButton(bot, "авто-подключение", autoURL, "", "🔗"),
			})
		}
	}

	rows = append(rows, []rawkbd.Button{
		instructionCallbackButton(bot, "выйти", "nav_instructions", instructionIconExitID, "✖️"),
	})

	kb := rawkbd.Markup{InlineKeyboard: rows}

	caption := steps[step].caption
	if step == keyStep {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n<code>%s</code>\n\n(нажмите чтобы копировать)", caption, html.EscapeString(key))
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
			_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, state.MessageID))
			state.MessageID = 0
		case !needsImage && !state.HasImage:
			if err := editInstructionText(bot, chatID, state.MessageID, caption, kb); err != nil {
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
	downloadURL := "https://play.google.com/store/apps/details?id=com.v2raytun.android&hl=ru"
	keyStep := 1

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

	var rows [][]rawkbd.Button
	var row []rawkbd.Button

	if step > 0 {
		row = append(row, instructionCallbackButton(bot, "назад", fmt.Sprintf("android_prev_%d", step), instructionIconBackID, "⬅️"))
	}
	row = append(row, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "android_current", "", ""))
	if step < len(steps)-1 {
		row = append(row, instructionCallbackButton(bot, "вперёд", fmt.Sprintf("android_next_%d", step), instructionIconNextID, "➡️"))
	}

	rows = append(rows, row)

	if step == 0 && strings.TrimSpace(downloadURL) != "" {
		linkRow := []rawkbd.Button{
			instructionURLButton(
				bot,
				"скачать",
				downloadURL,
				instructionIconDownloadID,
				"⬇️",
			),
		}
		rows = append(rows, linkRow)
	}

	rows = append(rows, []rawkbd.Button{
		instructionCallbackButton(bot, "выйти", "nav_instructions", instructionIconExitID, "✖️"),
	})

	kb := rawkbd.Markup{InlineKeyboard: rows}

	caption := steps[step].caption
	if step == keyStep {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n<code>%s</code>\n\n(нажмите чтобы копировать)", caption, html.EscapeString(key))
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
			_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, state.MessageID))
			state.MessageID = 0
		case !needsImage && !state.HasImage:
			if err := editInstructionText(bot, chatID, state.MessageID, caption, kb); err != nil {
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

	var rows [][]rawkbd.Button
	var row []rawkbd.Button

	if step > 0 {
		row = append(row, instructionCallbackButton(bot, "назад", fmt.Sprintf("ios_prev_%d", step), instructionIconBackID, "⬅️"))
	}
	row = append(row, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "ios_current", "", ""))
	if step < len(steps)-1 {
		row = append(row, instructionCallbackButton(bot, "вперёд", fmt.Sprintf("ios_next_%d", step), instructionIconNextID, "➡️"))
	}

	rows = append(rows, row)

	if step == 0 {
		linkRow := []rawkbd.Button{
			instructionURLButton(
				bot,
				"скачать",
				"https://apps.apple.com/kz/app/v2raytun/id6476628951",
				instructionIconDownloadID,
				"⬇️",
			),
		}
		rows = append(rows, linkRow)
	}

	rows = append(rows, []rawkbd.Button{
		instructionCallbackButton(bot, "выйти", "nav_instructions", instructionIconExitID, "✖️"),
	})

	kb := rawkbd.Markup{InlineKeyboard: rows}

	caption := steps[step].caption
	if step == 1 {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n<code>%s</code>\n\n(нажмите чтобы копировать)", caption, html.EscapeString(key))
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
			_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, state.MessageID))
			state.MessageID = 0
		case !needsImage && !state.HasImage:
			if err := editInstructionText(bot, chatID, state.MessageID, caption, kb); err != nil {
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

	var rows [][]rawkbd.Button
	var row []rawkbd.Button

	if step > 0 {
		row = append(row, instructionCallbackButton(bot, "назад", fmt.Sprintf("macos_prev_%d", step), instructionIconBackID, "⬅️"))
	}
	row = append(row, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "macos_current", "", ""))
	if step < len(steps)-1 {
		row = append(row, instructionCallbackButton(bot, "вперёд", fmt.Sprintf("macos_next_%d", step), instructionIconNextID, "➡️"))
	}

	rows = append(rows, row)

	if step == 0 {
		linkRow := []rawkbd.Button{
			instructionURLButton(
				bot,
				"скачать",
				"https://apps.apple.com/kz/app/v2raytun/id6476628951",
				instructionIconDownloadID,
				"⬇️",
			),
		}
		rows = append(rows, linkRow)
	}

	rows = append(rows, []rawkbd.Button{
		instructionCallbackButton(bot, "выйти", "nav_instructions", instructionIconExitID, "✖️"),
	})

	kb := rawkbd.Markup{InlineKeyboard: rows}

	caption := steps[step].caption
	if step == 1 {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n<code>%s</code>\n\n(нажмите чтобы копировать)", caption, html.EscapeString(key))
		}
	}

	state, ok := macosStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		if err := editInstructionText(bot, chatID, state.MessageID, caption, kb); err != nil {
			log.Printf("macos edit text failed: %v", err)
			state.MessageID = 0
		} else {
			state.CurrentStep = step
			state.HasImage = false
			macosStates[chatID] = state
			return state.MessageID, nil
		}
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

func sendInstructionPhoto(bot *tgbotapi.BotAPI, chatID int64, photoPath, caption string, kb rawkbd.Markup) (int, error) {
	photo := tgbotapi.NewPhoto(chatID, tgbotapi.FilePath(photoPath))
	photo.Caption = caption
	photo.ParseMode = "HTML"

	sent, err := bot.Send(photo)
	if err != nil {
		log.Printf("send photo failed: %v", err)
		return 0, err
	}

	// Update reply markup with raw keyboard
	if err := editInstructionReplyMarkup(bot, chatID, sent.MessageID, kb); err != nil {
		log.Printf("failed to update photo reply markup: %v", err)
	}

	return sent.MessageID, nil
}

func sendInstructionAnimation(bot *tgbotapi.BotAPI, chatID int64, animationPath, caption string, kb rawkbd.Markup) (int, error) {
	animation := tgbotapi.NewAnimation(chatID, tgbotapi.FilePath(animationPath))
	animation.Caption = caption
	animation.ParseMode = "HTML"

	sent, err := bot.Send(animation)
	if err != nil {
		log.Printf("send animation failed: %v", err)
		return 0, err
	}

	// Update reply markup with raw keyboard
	if err := editInstructionReplyMarkup(bot, chatID, sent.MessageID, kb); err != nil {
		log.Printf("failed to update animation reply markup: %v", err)
	}

	return sent.MessageID, nil
}

func isAnimationPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".mp4")
}

func sendInstructionText(bot *tgbotapi.BotAPI, chatID int64, text string, kb rawkbd.Markup) (int, error) {
	params := tgbotapi.Params{}
	params.AddNonZero64("chat_id", chatID)
	params["text"] = text
	params["parse_mode"] = "HTML"
	params.AddBool("disable_web_page_preview", true)
	if err := params.AddInterface("reply_markup", kb); err != nil {
		return 0, err
	}

	resp, err := bot.MakeRequest("sendMessage", params)
	if err != nil {
		log.Printf("send text failed: %v", err)
		return 0, err
	}
	if !resp.Ok {
		return 0, fmt.Errorf("telegram sendMessage error %d: %s", resp.ErrorCode, resp.Description)
	}

	var sent struct {
		MessageID int `json:"message_id"`
	}
	if err := json.Unmarshal(resp.Result, &sent); err != nil {
		return 0, err
	}
	if sent.MessageID == 0 {
		return 0, errors.New("telegram sendMessage returned empty message_id")
	}

	return sent.MessageID, nil
}

func editInstructionText(bot *tgbotapi.BotAPI, chatID int64, messageID int, text string, kb rawkbd.Markup) error {
	params := tgbotapi.Params{}
	params.AddNonZero64("chat_id", chatID)
	params.AddNonZero("message_id", messageID)
	params["text"] = text
	params["parse_mode"] = "HTML"
	params.AddBool("disable_web_page_preview", true)
	if err := params.AddInterface("reply_markup", kb); err != nil {
		return err
	}

	resp, err := bot.MakeRequest("editMessageText", params)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("telegram editMessageText error %d: %s", resp.ErrorCode, resp.Description)
	}

	return nil
}

func editInstructionReplyMarkup(bot *tgbotapi.BotAPI, chatID int64, messageID int, kb rawkbd.Markup) error {
	params := tgbotapi.Params{}
	params.AddNonZero64("chat_id", chatID)
	params.AddNonZero("message_id", messageID)
	if err := params.AddInterface("reply_markup", kb); err != nil {
		return err
	}

	resp, err := bot.MakeRequest("editMessageReplyMarkup", params)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("telegram editMessageReplyMarkup error %d: %s", resp.ErrorCode, resp.Description)
	}

	return nil
}

func editInstructionMedia(bot *tgbotapi.BotAPI, chatID int64, messageID int, mediaType, mediaPath, caption string, kb rawkbd.Markup) error {
	// Delete old message and send new one (simpler than editMessageMedia with files)
	_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, messageID))

	var msgID int
	var err error
	if mediaType == "animation" {
		msgID, err = sendInstructionAnimation(bot, chatID, mediaPath, caption, kb)
	} else {
		msgID, err = sendInstructionPhoto(bot, chatID, mediaPath, caption, kb)
	}

	if err != nil {
		return err
	}

	// Update message ID in state would happen at caller level
	_ = msgID
	return nil
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
