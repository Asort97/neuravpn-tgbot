package rawkbd

// Button is a raw Telegram inline button payload with custom emoji support.
type Button struct {
	Text              string  `json:"text"`
	CallbackData      *string `json:"callback_data,omitempty"`
	URL               *string `json:"url,omitempty"`
	Style             string  `json:"style,omitempty"`
	IconCustomEmojiID string  `json:"icon_custom_emoji_id,omitempty"`
	WebApp            *WebApp `json:"web_app,omitempty"`
}

type WebApp struct {
	URL string `json:"url"`
}

// Markup is a raw Telegram inline keyboard payload.
type Markup struct {
	InlineKeyboard [][]Button `json:"inline_keyboard"`
}

// CallbackButton creates a callback button.
func CallbackButton(text, callbackData, style, iconCustomEmojiID string) Button {
	cb := callbackData
	return Button{
		Text:              text,
		CallbackData:      &cb,
		Style:             style,
		IconCustomEmojiID: iconCustomEmojiID,
	}
}

// URLButton creates a URL button.
func URLButton(text, url, iconCustomEmojiID string) Button {
	u := url
	return Button{
		Text:              text,
		URL:               &u,
		IconCustomEmojiID: iconCustomEmojiID,
	}
}

func WebAppButton(text, url, iconCustomEmojiID string) Button {
	return Button{
		Text:              text,
		WebApp:            &WebApp{URL: url},
		IconCustomEmojiID: iconCustomEmojiID,
	}
}
