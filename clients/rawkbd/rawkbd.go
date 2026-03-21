package rawkbd

import "github.com/SevereCloud/vksdk/v3/object"

// Button represents a VK keyboard button abstraction.
type Button struct {
	Text         string
	CallbackData string // Non-empty for callback buttons
	URL          string // Non-empty for link buttons
	Color        string // primary, secondary, positive, negative
}

// Markup wraps a VK inline keyboard.
type Markup struct {
	Buttons [][]Button
}

// CallbackButton creates a callback button.
// style and iconCustomEmojiID are kept in the signature for source compatibility but ignored.
func CallbackButton(text, callbackData, style, iconCustomEmojiID string) Button {
	return Button{
		Text:         text,
		CallbackData: callbackData,
		Color:        "secondary",
	}
}

// URLButton creates a URL button.
func URLButton(text, url, iconCustomEmojiID string) Button {
	return Button{
		Text:  text,
		URL:   url,
		Color: "secondary",
	}
}

// ToVKKeyboard converts our Markup to a VK MessagesKeyboard.
func (m Markup) ToVKKeyboard() *object.MessagesKeyboard {
	kb := &object.MessagesKeyboard{
		Inline:  true,
		Buttons: make([][]object.MessagesKeyboardButton, 0, len(m.Buttons)),
	}
	for _, row := range m.Buttons {
		var vkRow []object.MessagesKeyboardButton
		for _, btn := range row {
			if btn.URL != "" {
				vkRow = append(vkRow, object.MessagesKeyboardButton{
					Action: object.MessagesKeyboardButtonAction{
						Type:  "open_link",
						Link:  btn.URL,
						Label: btn.Text,
					},
				})
			} else if btn.CallbackData != "" {
				vkRow = append(vkRow, object.MessagesKeyboardButton{
					Color: mapColor(btn.Color),
					Action: object.MessagesKeyboardButtonAction{
						Type:    "callback",
						Label:   btn.Text,
						Payload: `{"cmd":"` + escapePayload(btn.CallbackData) + `"}`,
					},
				})
			} else {
				vkRow = append(vkRow, object.MessagesKeyboardButton{
					Color: mapColor(btn.Color),
					Action: object.MessagesKeyboardButtonAction{
						Type:  "text",
						Label: btn.Text,
					},
				})
			}
		}
		if len(vkRow) > 0 {
			kb.Buttons = append(kb.Buttons, vkRow)
		}
	}
	return kb
}

func mapColor(c string) string {
	switch c {
	case "primary":
		return "primary"
	case "positive":
		return "positive"
	case "negative":
		return "negative"
	default:
		return "secondary"
	}
}

func escapePayload(s string) string {
	// Escape quotes for JSON payload
	result := ""
	for _, r := range s {
		if r == '"' {
			result += `\"`
		} else if r == '\\' {
			result += `\\`
		} else {
			result += string(r)
		}
	}
	return result
}
