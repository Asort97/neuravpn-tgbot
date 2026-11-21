package instruct

import (
	"fmt"
	"html"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/Asort97/vpnBot/clients/rawkbd"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

// Setup guides with a choice of client app per platform, mirroring neuravpn.ru/instructions.
// Callback data (all under 64 bytes):
//   ig:p:<platform>              app picker for a platform
//   ig:s:<platform>:<app>:<step> show a step of one app's guide

const guideClipDir = "InstructionPhotos/clips/"

// Official download sources only. Happ is no longer on Google Play (the listing under its
// name there is from another developer), so Android uses the APK from Happ's GitHub.
const (
	linkNeuravpnAndroid = "https://play.google.com/store/apps/details?id=com.neuravpn.app"
	linkNeuravpnWindows = "https://neuravpn.ru/#app"
	linkHappIOS         = "https://apps.apple.com/app/id6504287215"
	linkHappAndroid     = "https://github.com/Happ-proxy/happ-android/releases/latest/download/Happ.apk"
	linkHappWindows     = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe"
	linkHappMac         = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/Happ.macOS.universal.dmg"
	linkHappLinuxDeb    = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/Happ.linux.x64.deb"
	linkHappLinuxRpm    = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/Happ.linux.x64.rpm"
	linkIncyIOS         = "https://apps.apple.com/app/id6756943388"
	linkIncyAndroid     = "https://play.google.com/store/apps/details?id=llc.itdev.incy"
	linkIncyDesktop     = "https://github.com/INCY-DEV/incy-platforms/releases"
)

type guideButton struct {
	text     string
	url      string // URL button when set
	callback string // callback button otherwise
}

type guideStep struct {
	media   string
	caption string
	buttons []guideButton
	key     bool // append the user's key to the caption
}

type guideApp struct {
	id    string
	name  string
	badge string
	steps []guideStep
}

type guidePlatform struct {
	id    string
	title string
	apps  []guideApp
}

type guideState struct {
	messageID int
	hasMedia  bool
}

var (
	guideMu     sync.Mutex
	guideStates = make(map[int64]*guideState)
)

func clipPath(device, app, step string) string {
	return guideClipDir + device + "-" + app + "-" + step + ".mp4"
}

func keyStep() guideStep {
	return guideStep{caption: "<b>скопируйте ключ</b>\n\nключ подключения ниже — нажмите на него, чтобы скопировать. приложение возьмёт его из буфера обмена.", key: true}
}

func mobileGuide(device, app string) []guideStep {
	happ := app == "happ"
	name := map[bool]string{true: "Happ", false: "INCY"}[happ]
	var steps []guideStep
	if device == "ios" {
		steps = append(steps,
			guideStep{caption: "<b>смените регион App Store</b>\n\nв российском App Store " + name + " нет. переключите регион на Казахстан — это займёт пару минут, покупки и подписки сохранятся.",
				buttons: []guideButton{{text: "как сменить регион", callback: "change_region_ios"}}},
			guideStep{caption: "<b>установите " + name + "</b>\n\nскачайте " + name + " из App Store.",
				buttons: []guideButton{{text: "открыть App Store", url: map[bool]string{true: linkHappIOS, false: linkIncyIOS}[happ]}}})
	} else if happ {
		steps = append(steps, guideStep{caption: "<b>установите Happ</b>\n\nHapp удалили из Google Play, поэтому ставим официальный файл с GitHub разработчика. скачайте <b>Happ.apk</b>, откройте его и разрешите установку из этого источника.\n\n<b>важно:</b> приложения «Happ» в Google Play сейчас выпускают другие разработчики — их не ставьте.",
			buttons: []guideButton{{text: "скачать Happ.apk", url: linkHappAndroid}}})
	} else {
		steps = append(steps, guideStep{caption: "<b>установите INCY</b>\n\nскачайте INCY из Google Play. разработчик — LLC ITDEV.",
			buttons: []guideButton{{text: "открыть Google Play", url: linkIncyAndroid}}})
	}
	steps = append(steps, keyStep())
	add := "откройте INCY и нажмите <b>«Вставить»</b>. подписка <b>neuravpn</b> появится в разделе «Текущий провайдер»."
	if happ {
		add = "откройте Happ и нажмите <b>«Буфер обмена»</b>. подписка <b>neuravpn</b> появится в списке серверов."
	}
	allow := "подтвердите запрос на подключение"
	if device == "ios" {
		allow = "разрешите добавить конфигурацию VPN"
	}
	steps = append(steps,
		guideStep{media: clipPath(device, app, "add"), caption: "<b>добавьте ключ</b>\n\n" + add},
		guideStep{media: clipPath(device, app, "connect"), caption: "<b>подключитесь</b>\n\nнажмите на большую кнопку и " + allow + ". готово — сайты открываются."})
	return steps
}

func desktopGuide(device, app string) []guideStep {
	happ := app == "happ"
	var steps []guideStep
	switch {
	case happ && device == "windows":
		steps = append(steps, guideStep{caption: "<b>установите Happ</b>\n\nскачайте установщик <b>setup-Happ.x64.exe</b> с GitHub разработчика и установите приложение.",
			buttons: []guideButton{{text: "скачать для Windows", url: linkHappWindows}}})
	case happ && device == "macos":
		steps = append(steps, guideStep{caption: "<b>установите Happ</b>\n\nскачайте <b>Happ.macOS.universal.dmg</b>, откройте его и перетащите Happ в «Программы».",
			buttons: []guideButton{{text: "скачать для macOS", url: linkHappMac}}})
	case happ:
		steps = append(steps, guideStep{caption: "<b>установите Happ</b>\n\nскачайте пакет для своего дистрибутива: <b>.deb</b> для Ubuntu и Debian, <b>.rpm</b> для Fedora и openSUSE. для Arch есть пакет <b>happ-desktop</b> в AUR.",
			buttons: []guideButton{{text: "скачать .deb", url: linkHappLinuxDeb}, {text: "скачать .rpm", url: linkHappLinuxRpm}}})
	default:
		file := map[string]string{
			"windows": "<b>incy-windows-setup.exe</b>",
			"macos":   "<b>incy-macos-arm64.dmg</b> (Mac на M1 и новее) или <b>incy-macos-intel.dmg</b>",
			"linux":   "<b>incy-linux-x64.deb</b>, <b>.rpm</b> или <b>.pkg.tar.zst</b> для своего дистрибутива",
		}[device]
		steps = append(steps, guideStep{caption: "<b>установите INCY</b>\n\nоткройте страницу релизов INCY, найдите последний релиз <b>Desktop</b> и скачайте " + file + ".",
			buttons: []guideButton{{text: "открыть релизы INCY", url: linkIncyDesktop}}})
	}
	steps = append(steps, keyStep())
	add := "в INCY нажмите <b>«Вставить»</b>. подписка <b>neuravpn</b> появится в разделе «Текущий провайдер»."
	if happ {
		add = "в Happ нажмите <b>«+»</b> и выберите <b>«Буфер обмена»</b>. подписка <b>neuravpn</b> появится в списке серверов."
	}
	steps = append(steps,
		guideStep{media: clipPath(device, app, "add"), caption: "<b>добавьте ключ</b>\n\n" + add},
		guideStep{media: clipPath(device, app, "connect"), caption: "<b>подключитесь</b>\n\nнажмите на большую кнопку. когда появится «Подключено», VPN работает."})
	return steps
}

var neuravpnAndroidGuide = []guideStep{
	{caption: "<b>установите neuravpn</b>\n\nскачайте приложение neuravpn из Google Play.",
		buttons: []guideButton{{text: "открыть Google Play", url: linkNeuravpnAndroid}}},
	keyStep(),
	{media: clipPath("android", "neuravpn", "add"), caption: "<b>добавьте ключ</b>\n\nоткройте neuravpn и нажмите <b>«Вставить из буфера обмена»</b>. подключение появится на главном экране."},
	{media: clipPath("android", "neuravpn", "connect"), caption: "<b>подключитесь</b>\n\nнажмите на большую кнопку с логотипом и подтвердите запрос на подключение. когда кнопка станет красной, VPN работает."},
}

var neuravpnWindowsGuide = []guideStep{
	{media: "InstructionPhotos/Windows/neuravpn_app/0.png", caption: "<b>скачайте neuravpn</b>\n\nскачайте установщик neuravpn для Windows на сайте — кнопка «Скачать для Windows».",
		buttons: []guideButton{{text: "открыть сайт", url: linkNeuravpnWindows}}},
	{caption: "<b>установите и откройте</b>\n\nзапустите файл <code>neuravpn-setup</code>, установите приложение и откройте его. если Windows попросит разрешение, подтвердите запуск."},
	{media: "InstructionPhotos/Windows/0.mp4", caption: "<b>вставьте ключ</b>\n\nскопируйте ключ ниже и в программе нажмите «вставить из буфера».", key: true},
	{media: "InstructionPhotos/Windows/1.mp4", caption: "<b>подключитесь</b>\n\nнажмите на большую кнопку в центре."},
}

var guidePlatforms = map[string]guidePlatform{
	"android": {id: "android", title: "Android", apps: []guideApp{
		{id: "neuravpn", name: "neuravpn", badge: "рекомендуем", steps: neuravpnAndroidGuide},
		{id: "happ", name: "Happ", steps: mobileGuide("android", "happ")},
		{id: "incy", name: "INCY", steps: mobileGuide("android", "incy")}}},
	"ios": {id: "ios", title: "iPhone и iPad", apps: []guideApp{
		{id: "happ", name: "Happ", steps: mobileGuide("ios", "happ")},
		{id: "incy", name: "INCY", steps: mobileGuide("ios", "incy")}}},
	"windows": {id: "windows", title: "Windows", apps: []guideApp{
		{id: "neuravpn", name: "neuravpn", badge: "рекомендуем", steps: neuravpnWindowsGuide},
		{id: "happ", name: "Happ", steps: desktopGuide("windows", "happ")},
		{id: "incy", name: "INCY", steps: desktopGuide("windows", "incy")}}},
	"macos": {id: "macos", title: "macOS", apps: []guideApp{
		{id: "happ", name: "Happ", steps: desktopGuide("macos", "happ")},
		{id: "incy", name: "INCY", steps: desktopGuide("macos", "incy")}}},
	"linux": {id: "linux", title: "Linux", apps: []guideApp{
		{id: "happ", name: "Happ", steps: desktopGuide("linux", "happ")},
		{id: "incy", name: "INCY", steps: desktopGuide("linux", "incy")}}},
}

// IsGuidePlatform reports whether the callback data names a guide platform.
func IsGuidePlatform(data string) bool {
	_, ok := guidePlatforms[data]
	return ok
}

// IsGuideCallback reports whether the callback data belongs to the guide flow.
func IsGuideCallback(data string) bool { return strings.HasPrefix(data, "ig:") }

// IsGuideStepCallback reports step navigation, which callers usually do not log.
func IsGuideStepCallback(data string) bool { return strings.HasPrefix(data, "ig:s:") }

// ShowGuideApps sends or edits the app picker for a platform and returns its message ID.
func ShowGuideApps(chatID int64, bot *tgbotapi.BotAPI, platform string) (int, error) {
	p, ok := guidePlatforms[platform]
	if !ok {
		return 0, fmt.Errorf("unknown guide platform %q", platform)
	}
	var rows [][]rawkbd.Button
	for _, a := range p.apps {
		label := a.name
		if a.badge != "" {
			label += " · " + a.badge
		}
		rows = append(rows, []rawkbd.Button{rawkbd.CallbackButton(label, "ig:s:"+p.id+":"+a.id+":0", "", "")})
	}
	rows = append(rows, []rawkbd.Button{instructionCallbackButton(bot, "назад", "nav_instructions", instructionIconBackID, "⬅️")})
	text := "<b>" + html.EscapeString(p.title) + "</b>\n\nвыберите приложение:"
	if len(p.apps) == 3 {
		text += "\n\nneuravpn — наше приложение, подключается в пару нажатий. Happ и INCY — запасные варианты."
	}
	return showGuideMessage(chatID, bot, "", text, rawkbd.Markup{InlineKeyboard: rows})
}

// HandleGuideCallback processes ig: callbacks and returns the message ID now on screen.
func HandleGuideCallback(chatID int64, bot *tgbotapi.BotAPI, data string) (int, error) {
	parts := strings.Split(data, ":")
	switch {
	case len(parts) == 3 && parts[1] == "p":
		return ShowGuideApps(chatID, bot, parts[2])
	case len(parts) == 5 && parts[1] == "s":
		step, err := strconv.Atoi(parts[4])
		if err != nil {
			return 0, err
		}
		return showGuideStep(chatID, bot, parts[2], parts[3], step)
	}
	return 0, fmt.Errorf("unknown guide callback %q", data)
}

func showGuideStep(chatID int64, bot *tgbotapi.BotAPI, platform, appID string, step int) (int, error) {
	p, ok := guidePlatforms[platform]
	if !ok {
		return 0, fmt.Errorf("unknown guide platform %q", platform)
	}
	var app *guideApp
	for i := range p.apps {
		if p.apps[i].id == appID {
			app = &p.apps[i]
		}
	}
	if app == nil {
		return 0, fmt.Errorf("unknown guide app %q", appID)
	}
	if step < 0 {
		step = 0
	}
	if step >= len(app.steps) {
		step = len(app.steps) - 1
	}
	s := app.steps[step]

	base := "ig:s:" + p.id + ":" + app.id + ":"
	var nav []rawkbd.Button
	if step > 0 {
		nav = append(nav, instructionCallbackButton(bot, "назад", base+strconv.Itoa(step-1), instructionIconBackID, "⬅️"))
	}
	nav = append(nav, rawkbd.CallbackButton(fmt.Sprintf("шаг %d/%d", step+1, len(app.steps)), "ig_current", "", ""))
	if step < len(app.steps)-1 {
		nav = append(nav, instructionCallbackButton(bot, "вперёд", base+strconv.Itoa(step+1), instructionIconNextID, "➡️"))
	}
	rows := [][]rawkbd.Button{nav}
	for _, b := range s.buttons {
		if b.url != "" {
			rows = append(rows, []rawkbd.Button{instructionURLButton(bot, b.text, b.url, instructionIconDownloadID, "⬇️")})
		} else {
			rows = append(rows, []rawkbd.Button{rawkbd.CallbackButton(b.text, b.callback, "", "")})
		}
	}
	rows = append(rows, []rawkbd.Button{
		rawkbd.CallbackButton("другое приложение", "ig:p:"+p.id, "", ""),
		instructionCallbackButton(bot, "выйти", "nav_instructions", instructionIconExitID, "✖️"),
	})

	caption := fmt.Sprintf("<b>%s · %s</b>\n%s", html.EscapeString(p.title), html.EscapeString(app.name), s.caption)
	if s.key {
		if key := strings.TrimSpace(instructionKeys[chatID]); key != "" {
			caption += fmt.Sprintf("\n\n<code>%s</code>\n\n<tg-emoji emoji-id=\"5264948349420739524\">✏️</tg-emoji> нажмите, чтобы скопировать", html.EscapeString(key))
		} else {
			caption += "\n\nключ появится после оформления доступа — откройте «получить VPN» в меню."
		}
	}
	return showGuideMessage(chatID, bot, s.media, caption, rawkbd.Markup{InlineKeyboard: rows})
}

// showGuideMessage edits the guide message in place when the media kind allows it,
// otherwise replaces it, and remembers which message is on screen.
func showGuideMessage(chatID int64, bot *tgbotapi.BotAPI, media, text string, kb rawkbd.Markup) (int, error) {
	guideMu.Lock()
	st := guideStates[chatID]
	if st == nil {
		st = &guideState{}
		guideStates[chatID] = st
	}
	guideMu.Unlock()

	hasMedia := strings.TrimSpace(media) != ""
	if st.messageID != 0 {
		var err error
		switch {
		case hasMedia && st.hasMedia:
			err = editInstructionMediaInPlace(bot, chatID, st.messageID, media, text, kb)
		case !hasMedia && !st.hasMedia:
			err = editInstructionText(bot, chatID, st.messageID, text, kb)
		default:
			err = fmt.Errorf("media kind changed")
		}
		if err == nil {
			st.hasMedia = hasMedia
			return st.messageID, nil
		}
		if !strings.Contains(err.Error(), "media kind changed") {
			log.Printf("guide edit failed, resending: %v", err)
		}
		_, _ = bot.Send(tgbotapi.NewDeleteMessage(chatID, st.messageID))
		st.messageID = 0
	}

	var (
		msgID int
		err   error
	)
	switch {
	case hasMedia && isAnimationPath(media):
		msgID, err = sendInstructionAnimation(bot, chatID, media, text, kb)
	case hasMedia:
		msgID, err = sendInstructionPhoto(bot, chatID, media, text, kb)
	default:
		msgID, err = sendInstructionText(bot, chatID, text, kb)
	}
	if err != nil {
		return 0, err
	}
	st.messageID = msgID
	st.hasMedia = hasMedia
	return msgID, nil
}

// SetGuideMessage records a message already on screen (for example the platform menu)
// so the guide edits it instead of sending a new one.
func SetGuideMessage(chatID int64, messageID int, hasMedia bool) {
	guideMu.Lock()
	defer guideMu.Unlock()
	guideStates[chatID] = &guideState{messageID: messageID, hasMedia: hasMedia}
}

func resetGuideState(chatID int64) {
	guideMu.Lock()
	defer guideMu.Unlock()
	delete(guideStates, chatID)
}

// GuideOnScreenHasMedia reports whether the guide message now on screen carries media.
func GuideOnScreenHasMedia(chatID int64) bool {
	guideMu.Lock()
	defer guideMu.Unlock()
	st := guideStates[chatID]
	return st != nil && st.hasMedia
}
