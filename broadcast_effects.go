package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

const confettiEffectID = "5046509860389126442"

// Only a leading explicit option enables an effect; ordinary emoji remain text.
func parseBroadcastEffect(text string) (string, string, error) {
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "--effect") {
		return text, "", nil
	}
	parts := strings.SplitN(text, " ", 2)
	if parts[0] != "--effect" || len(parts) != 2 {
		return "", "", fmt.Errorf("использование: /notify-test --effect confetti текст")
	}
	rest := strings.TrimSpace(parts[1])
	i := strings.IndexFunc(rest, unicode.IsSpace)
	name, body := rest, ""
	if i >= 0 {
		name, body = rest[:i], strings.TrimSpace(rest[i:])
	}
	if name != "confetti" && name != "🎉" {
		return "", "", fmt.Errorf("неизвестный эффект; используйте --effect confetti")
	}
	return body, confettiEffectID, nil
}

func broadcastEffectParams(target int64, source *tgbotapi.Message, text string, preserve bool, effect string) (string, tgbotapi.Params, error) {
	if target <= 0 || source == nil || effect != confettiEffectID {
		return "", nil, fmt.Errorf("invalid effect destination or source")
	}
	p := tgbotapi.Params{"chat_id": strconv.FormatInt(target, 10), "message_effect_id": effect}
	if preserve {
		if source.Chat == nil {
			return "", nil, fmt.Errorf("missing source chat")
		}
		p["from_chat_id"] = strconv.FormatInt(source.Chat.ID, 10)
		p["message_id"] = strconv.Itoa(source.MessageID)
		return "copyMessage", p, nil
	}
	method, key, id := "", "", ""
	switch {
	case source.Animation != nil:
		method, key, id = "sendAnimation", "animation", source.Animation.FileID
	case len(source.Photo) > 0:
		method, key, id = "sendPhoto", "photo", source.Photo[len(source.Photo)-1].FileID
	case source.Video != nil:
		method, key, id = "sendVideo", "video", source.Video.FileID
	case source.Document != nil:
		method, key, id = "sendDocument", "document", source.Document.FileID
	}
	p["parse_mode"] = "HTML"
	if method != "" {
		if text == "" {
			text = source.Caption
			if cmd, _ := parseLeadingSlashCommand(text); isNotifyCommandName(cmd) {
				text = ""
			}
		}
		p[key] = id
		p["caption"] = text
		return method, p, nil
	}
	if strings.TrimSpace(text) == "" {
		return "", nil, fmt.Errorf("broadcast text is empty")
	}
	p["text"] = text
	return "sendMessage", p, nil
}
