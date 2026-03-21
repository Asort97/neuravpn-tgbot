package instruct

import (
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"

	"github.com/Asort97/vpnBot/clients/rawkbd"
	"github.com/Asort97/vpnBot/clients/vkbot"
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
	state := &InstructionState{
		CurrentStep: -1,
		MessageID:   messageID,
		ChatID:      chatID,
		HasImage:    false,
	}
	switch instructType {
	case Windows:
		windowsStates[chatID] = state
	case Android:
		androidStates[chatID] = state
	case IOS:
		iosStates[chatID] = state
	case MacOS:
		macosStates[chatID] = state
	}
}

// SetInstructionKey stores a per-chat key used in captions.
func SetInstructionKey(chatID int64, key string) {
	instructionKeys[chatID] = key
}

// ValidateCustomEmojiIDs is a no-op for VK (VK doesn't support custom emoji in bots).
func ValidateCustomEmojiIDs(_ *vkbot.Bot) {}

// stripHTML removes HTML tags and replaces common entities for VK plain text messages.
var htmlTagRegex = regexp.MustCompile(`<[^>]*>`)

func stripHTML(s string) string {
	// Extract href from <a> tags and append URL in parentheses
	linkRegex := regexp.MustCompile(`<a\s+href="([^"]*)"[^>]*>(.*?)</a>`)
	s = linkRegex.ReplaceAllString(s, "$2 ($1)")
	// Remove remaining HTML tags
	s = htmlTagRegex.ReplaceAllString(s, "")
	return s
}

func callbackBtn(text, callbackData, fallbackEmoji string) rawkbd.Button {
	if fallbackEmoji != "" {
		text = fallbackEmoji + " " + text
	}
	return rawkbd.CallbackButton(text, callbackData, "", "")
}

func urlBtn(text, url, fallbackEmoji string) rawkbd.Button {
	if fallbackEmoji != "" {
		text = fallbackEmoji + " " + text
	}
	return rawkbd.URLButton(text, url, "")
}

func InstructionWindows(chatID int64, bot *vkbot.Bot, step int) (int, error) {
	downloadURL := "https://asort97.github.io/neuravpn-site"
	keyStep := 3

	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Windows/neuravpn_app/0.png", "скачайте последнюю версию neuravpn c сайта (https://asort97.github.io/neuravpn-site/), нажав кнопку «скачать для windows»"},
		{"", "после завершения загрузки выполните следующие действия:\n\n1) найдите загруженный файл neuravpn_windows_vX.X.X.\n2) щелкните правой кнопкой мыши на файле и выберите «извлечь все»."},
		{"", "откройте папку с распакованными файлами. найдите файл neuravpn.exe. щелкните по нему правой кнопкой мыши и запустите от имени администратора."},
		{"InstructionPhotos/Windows/0.mp4", "предварительно скопировав ключ доступа, в программе нажмите на кнопку «вставить из буфера» или просто нажмите на кнопку ниже «авто-подключение»"},
		{"InstructionPhotos/Windows/1.mp4", "подключитесь к vpn, нажав по большой кнопке в центру."},
	}

	if step < 0 {
		step = 0
	}
	if step >= len(steps) {
		step = len(steps) - 1
	}

	var rows [][]rawkbd.Button
	var row []rawkbd.Button

	if step > 0 {
		row = append(row, callbackBtn("назад", fmt.Sprintf("win_prev_%d", step), "⬅️"))
	}
	row = append(row, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "win_current", "", ""))
	if step < len(steps)-1 {
		row = append(row, callbackBtn("вперёд", fmt.Sprintf("win_next_%d", step), "➡️"))
	}
	rows = append(rows, row)

	if step == 0 && strings.TrimSpace(downloadURL) != "" {
		rows = append(rows, []rawkbd.Button{
			urlBtn("скачать", downloadURL, "⬇️"),
		})
	}

	if step == keyStep {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			autoURL := "https://asort97.github.io/neuravpn-site/?open=1&auto=1&v=" + url.QueryEscape(key)
			rows = append(rows, []rawkbd.Button{
				urlBtn("авто-подключение", autoURL, "🔗"),
			})
		}
	}

	rows = append(rows, []rawkbd.Button{
		callbackBtn("выйти", "nav_instructions", "✖️"),
	})

	kb := rawkbd.Markup{Buttons: rows}

	caption := steps[step].caption
	if step == keyStep {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n%s\n\n✏️ нажмите чтобы копировать", caption, key)
		}
	}

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, exists := windowsStates[chatID]
	if !exists || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		if !needsImage && !state.HasImage {
			vkKb := kb.ToVKKeyboard()
			if err := bot.EditMessage(int(chatID), state.MessageID, caption, vkKb); err != nil {
				log.Printf("windows edit text failed: %v", err)
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = false
				windowsStates[chatID] = state
				return state.MessageID, nil
			}
		}
		if state.MessageID != 0 {
			_ = bot.DeleteMessage(int(chatID), []int{state.MessageID})
			state.MessageID = 0
		}
	}

	vkKb := kb.ToVKKeyboard()
	var msgID int
	var err error

	if needsImage {
		msgID, err = bot.SendMedia(int(chatID), steps[step].photoPath, caption, vkKb)
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = bot.SendMessage(int(chatID), caption, vkKb)
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

func InstructionAndroid(chatID int64, bot *vkbot.Bot, step int) (int, error) {
	downloadURL := "https://play.google.com/store/apps/details?id=com.v2raytun.android&hl=ru"
	keyStep := 1

	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Android/0.MP4", "скачайте v2raytun из google play (https://play.google.com/store/apps/details?id=com.v2raytun.android&hl=ru)"},
		{"InstructionPhotos/Android/1.MP4", "заходим в приложение и вставляем ключ из буфера обмена. предварительно вы должны скопировать ключ-подключения который мы вам отправили"},
		{"InstructionPhotos/Android/2.MP4", "далее жмём на кнопку включения и vpn работает."},
	}

	if step < 0 {
		step = 0
	}
	if step >= len(steps) {
		step = len(steps) - 1
	}

	var rows [][]rawkbd.Button
	var row []rawkbd.Button

	if step > 0 {
		row = append(row, callbackBtn("назад", fmt.Sprintf("android_prev_%d", step), "⬅️"))
	}
	row = append(row, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "android_current", "", ""))
	if step < len(steps)-1 {
		row = append(row, callbackBtn("вперёд", fmt.Sprintf("android_next_%d", step), "➡️"))
	}
	rows = append(rows, row)

	if step == 0 && strings.TrimSpace(downloadURL) != "" {
		rows = append(rows, []rawkbd.Button{
			urlBtn("скачать", downloadURL, "⬇️"),
		})
	}

	rows = append(rows, []rawkbd.Button{
		callbackBtn("выйти", "nav_instructions", "✖️"),
	})

	kb := rawkbd.Markup{Buttons: rows}

	caption := steps[step].caption
	if step == keyStep {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n%s\n\n✏️ нажмите чтобы копировать", caption, key)
		}
	}

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, ok := androidStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		if !needsImage && !state.HasImage {
			vkKb := kb.ToVKKeyboard()
			if err := bot.EditMessage(int(chatID), state.MessageID, caption, vkKb); err != nil {
				log.Printf("android edit text failed: %v", err)
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = false
				androidStates[chatID] = state
				return state.MessageID, nil
			}
		}
		if state.MessageID != 0 {
			_ = bot.DeleteMessage(int(chatID), []int{state.MessageID})
			state.MessageID = 0
		}
	}

	vkKb := kb.ToVKKeyboard()
	var msgID int
	var err error

	if needsImage {
		msgID, err = bot.SendMedia(int(chatID), steps[step].photoPath, caption, vkKb)
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = bot.SendMessage(int(chatID), caption, vkKb)
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

func InstructionIos(chatID int64, bot *vkbot.Bot, step int) (int, error) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"InstructionPhotos/Ios/0.MP4", "скачайте v2raytun из app store (https://apps.apple.com/kz/app/v2raytun/id6476628951)"},
		{"InstructionPhotos/Ios/1.MP4", "заходим в приложение и вставляем ключ из буфера обмена. предварительно вы должны скопировать ключ-подключения который мы вам отправили"},
		{"InstructionPhotos/Ios/2.MP4", "далее жмём на кнопку включения и vpn работает."},
	}

	if step < 0 {
		step = 0
	}
	if step >= len(steps) {
		step = len(steps) - 1
	}

	var rows [][]rawkbd.Button
	var row []rawkbd.Button

	if step > 0 {
		row = append(row, callbackBtn("назад", fmt.Sprintf("ios_prev_%d", step), "⬅️"))
	}
	row = append(row, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "ios_current", "", ""))
	if step < len(steps)-1 {
		row = append(row, callbackBtn("вперёд", fmt.Sprintf("ios_next_%d", step), "➡️"))
	}
	rows = append(rows, row)

	if step == 0 {
		rows = append(rows, []rawkbd.Button{
			urlBtn("скачать", "https://apps.apple.com/kz/app/v2raytun/id6476628951", "⬇️"),
		})
	}

	rows = append(rows, []rawkbd.Button{
		callbackBtn("выйти", "nav_instructions", "✖️"),
	})

	kb := rawkbd.Markup{Buttons: rows}

	caption := steps[step].caption
	if step == 1 {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n%s\n\n✏️ нажмите чтобы копировать", caption, key)
		}
	}

	needsImage := strings.TrimSpace(steps[step].photoPath) != ""
	state, ok := iosStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		if !needsImage && !state.HasImage {
			vkKb := kb.ToVKKeyboard()
			if err := bot.EditMessage(int(chatID), state.MessageID, caption, vkKb); err != nil {
				log.Printf("ios edit text failed: %v", err)
				state.MessageID = 0
			} else {
				state.CurrentStep = step
				state.HasImage = false
				iosStates[chatID] = state
				return state.MessageID, nil
			}
		}
		if state.MessageID != 0 {
			_ = bot.DeleteMessage(int(chatID), []int{state.MessageID})
			state.MessageID = 0
		}
	}

	vkKb := kb.ToVKKeyboard()
	var msgID int
	var err error

	if needsImage {
		msgID, err = bot.SendMedia(int(chatID), steps[step].photoPath, caption, vkKb)
		if err != nil {
			return 0, err
		}
		state.HasImage = true
	} else {
		msgID, err = bot.SendMessage(int(chatID), caption, vkKb)
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

func InstructionMacOS(chatID int64, bot *vkbot.Bot, step int) (int, error) {
	steps := []struct {
		photoPath string
		caption   string
	}{
		{"", "скачайте v2raytun из app store (https://apps.apple.com/kz/app/v2raytun/id6476628951)"},
		{"", "заходим в приложение и вставляем ключ из буфера обмена. предварительно вы должны скопировать ключ-подключения который мы вам отправили"},
		{"", "далее жмём на кнопку включения и vpn работает."},
	}

	if step < 0 {
		step = 0
	}
	if step >= len(steps) {
		step = len(steps) - 1
	}

	var rows [][]rawkbd.Button
	var row []rawkbd.Button

	if step > 0 {
		row = append(row, callbackBtn("назад", fmt.Sprintf("macos_prev_%d", step), "⬅️"))
	}
	row = append(row, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(steps)), "macos_current", "", ""))
	if step < len(steps)-1 {
		row = append(row, callbackBtn("вперёд", fmt.Sprintf("macos_next_%d", step), "➡️"))
	}
	rows = append(rows, row)

	if step == 0 {
		rows = append(rows, []rawkbd.Button{
			urlBtn("скачать", "https://apps.apple.com/kz/app/v2raytun/id6476628951", "⬇️"),
		})
	}

	rows = append(rows, []rawkbd.Button{
		callbackBtn("выйти", "nav_instructions", "✖️"),
	})

	kb := rawkbd.Markup{Buttons: rows}

	caption := steps[step].caption
	if step == 1 {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption = fmt.Sprintf("%s\n\n%s\n\n✏️ нажмите чтобы копировать", caption, key)
		}
	}

	state, ok := macosStates[chatID]
	if !ok || state == nil {
		state = &InstructionState{ChatID: chatID}
	}

	if state.MessageID != 0 {
		vkKb := kb.ToVKKeyboard()
		if err := bot.EditMessage(int(chatID), state.MessageID, caption, vkKb); err != nil {
			log.Printf("macos edit text failed: %v", err)
			state.MessageID = 0
		} else {
			state.CurrentStep = step
			state.HasImage = false
			macosStates[chatID] = state
			return state.MessageID, nil
		}
	}

	vkKb := kb.ToVKKeyboard()
	msgID, err := bot.SendMessage(int(chatID), caption, vkKb)
	if err != nil {
		return 0, err
	}
	state.CurrentStep = step
	state.MessageID = msgID
	state.HasImage = false
	macosStates[chatID] = state
	return msgID, nil
}

func ResetState(chatID int64) {
	delete(windowsStates, chatID)
	delete(androidStates, chatID)
	delete(iosStates, chatID)
	delete(macosStates, chatID)
	delete(instructionKeys, chatID)
}
