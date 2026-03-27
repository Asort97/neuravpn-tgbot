package vkbot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/SevereCloud/vksdk/v3/api"
	"github.com/SevereCloud/vksdk/v3/object"
)

// Bot wraps VK API with helper methods similar to Telegram bot API.
type Bot struct {
	VK      *api.VK
	GroupID int

	// photoCache caches uploaded photo attachments keyed by file path.
	photoCacheMu sync.RWMutex
	photoCache   map[string]string // filePath -> "photo<owner>_<id>"

	// docCache caches uploaded doc attachments keyed by file path.
	docCacheMu sync.RWMutex
	docCache   map[string]string // filePath -> "doc<owner>_<id>"
}

// New creates a new VK Bot wrapper.
func New(vk *api.VK, groupID int) *Bot {
	return &Bot{
		VK:         vk,
		GroupID:    groupID,
		photoCache: make(map[string]string),
		docCache:   make(map[string]string),
	}
}

// randomID generates a random message ID (VK requirement).
func randomID() int {
	return rand.Int()
}

// SendMessage sends a text message with optional keyboard to a peer.
// Returns the message ID (conversation_message_id) or error.
func (b *Bot) SendMessage(peerID int, text string, keyboard *object.MessagesKeyboard) (int, error) {
	params := api.Params{
		"peer_id":   peerID,
		"message":   text,
		"random_id": randomID(),
	}
	if keyboard != nil {
		kb, err := json.Marshal(keyboard)
		if err != nil {
			return 0, fmt.Errorf("marshal keyboard: %w", err)
		}
		params["keyboard"] = string(kb)
	}
	msgID, err := b.VK.MessagesSend(params)
	if err != nil {
		return 0, err
	}
	return msgID, nil
}

// EditMessage edits an existing message by its message_id (not conversation_message_id).
func (b *Bot) EditMessage(peerID, messageID int, text string, keyboard *object.MessagesKeyboard) error {
	params := api.Params{
		"peer_id":    peerID,
		"message_id": messageID,
		"message":    text,
	}
	if keyboard != nil {
		kb, err := json.Marshal(keyboard)
		if err != nil {
			return fmt.Errorf("marshal keyboard: %w", err)
		}
		params["keyboard"] = string(kb)
	}
	_, err := b.VK.MessagesEdit(params)
	return err
}

// DeleteMessage deletes a message by ID.
func (b *Bot) DeleteMessage(peerID int, messageIDs []int) error {
	params := api.Params{
		"peer_id":        peerID,
		"message_ids":    messageIDs,
		"delete_for_all": 1,
	}
	_, err := b.VK.MessagesDelete(params)
	return err
}

// SendEventAnswer sends a snackbar response to a MessageEvent callback button press.
func (b *Bot) SendEventAnswer(eventID string, peerID, userID int, text string) error {
	eventData := map[string]string{}
	if text != "" {
		eventData["type"] = "show_snackbar"
		eventData["text"] = text
	}
	data, _ := json.Marshal(eventData)
	params := api.Params{
		"event_id":   eventID,
		"user_id":    userID,
		"peer_id":    peerID,
		"event_data": string(data),
	}
	_, err := b.VK.MessagesSendMessageEventAnswer(params)
	return err
}

// UploadPhoto uploads a photo from file path and returns the attachment string.
// Results are cached by file path to avoid re-uploading.
func (b *Bot) UploadPhoto(peerID int, filePath string) (string, error) {
	// Check cache
	b.photoCacheMu.RLock()
	if att, ok := b.photoCache[filePath]; ok {
		b.photoCacheMu.RUnlock()
		return att, nil
	}
	b.photoCacheMu.RUnlock()

	// Get upload server
	server, err := b.VK.PhotosGetMessagesUploadServer(api.Params{
		"peer_id": peerID,
	})
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	// Read file
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	// Create multipart request
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("photo", filepath.Base(filePath))
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(fw, f); err != nil {
		return "", fmt.Errorf("copy file: %w", err)
	}
	w.Close()

	// Upload
	req, err := http.NewRequest("POST", server.UploadURL, &buf)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var uploadResp struct {
		Server int    `json:"server"`
		Photo  string `json:"photo"`
		Hash   string `json:"hash"`
	}
	if err := json.Unmarshal(body, &uploadResp); err != nil {
		return "", fmt.Errorf("unmarshal upload response: %w", err)
	}

	// Save photo
	photos, err := b.VK.PhotosSaveMessagesPhoto(api.Params{
		"server": uploadResp.Server,
		"photo":  uploadResp.Photo,
		"hash":   uploadResp.Hash,
	})
	if err != nil {
		return "", fmt.Errorf("save photo: %w", err)
	}
	if len(photos) == 0 {
		return "", fmt.Errorf("no photos returned from save")
	}

	att := fmt.Sprintf("photo%d_%d", photos[0].OwnerID, photos[0].ID)
	if photos[0].AccessKey != "" {
		att += "_" + photos[0].AccessKey
	}

	// Cache
	b.photoCacheMu.Lock()
	b.photoCache[filePath] = att
	b.photoCacheMu.Unlock()

	return att, nil
}

// UploadDoc uploads a document (e.g. video/mp4) and returns the attachment string.
// Results are cached by file path.
func (b *Bot) UploadDoc(peerID int, filePath string) (string, error) {
	// Check cache
	b.docCacheMu.RLock()
	if att, ok := b.docCache[filePath]; ok {
		b.docCacheMu.RUnlock()
		return att, nil
	}
	b.docCacheMu.RUnlock()

	// Get upload server
	server, err := b.VK.DocsGetMessagesUploadServer(api.Params{
		"peer_id": peerID,
		"type":    "doc",
	})
	if err != nil {
		return "", fmt.Errorf("get doc upload server: %w", err)
	}

	// Read file
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	// Create multipart request
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := io.Copy(fw, f); err != nil {
		return "", fmt.Errorf("copy file: %w", err)
	}
	w.Close()

	// Upload
	req, err := http.NewRequest("POST", server.UploadURL, &buf)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var uploadResp struct {
		File string `json:"file"`
	}
	if err := json.Unmarshal(body, &uploadResp); err != nil {
		return "", fmt.Errorf("unmarshal upload response: %w", err)
	}

	// Save doc
	saveResp, err := b.VK.DocsSave(api.Params{
		"file": uploadResp.File,
	})
	if err != nil {
		return "", fmt.Errorf("save doc: %w", err)
	}

	att := fmt.Sprintf("doc%d_%d", saveResp.Doc.OwnerID, saveResp.Doc.ID)
	if saveResp.Doc.AccessKey != "" {
		att += "_" + saveResp.Doc.AccessKey
	}

	// Cache
	b.docCacheMu.Lock()
	b.docCache[filePath] = att
	b.docCacheMu.Unlock()

	return att, nil
}

// UploadPhotoBytes uploads a photo from raw bytes and returns the attachment string.
func (b *Bot) UploadPhotoBytes(peerID int, data []byte, filename string) (string, error) {
	server, err := b.VK.PhotosGetMessagesUploadServer(api.Params{
		"peer_id": peerID,
	})
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	fw, err := w.CreateFormFile("photo", filename)
	if err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	if _, err := fw.Write(data); err != nil {
		return "", fmt.Errorf("write data: %w", err)
	}
	w.Close()

	req, err := http.NewRequest("POST", server.UploadURL, &buf)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var uploadResp struct {
		Server int    `json:"server"`
		Photo  string `json:"photo"`
		Hash   string `json:"hash"`
	}
	if err := json.Unmarshal(body, &uploadResp); err != nil {
		return "", fmt.Errorf("unmarshal upload response: %w", err)
	}

	photos, err := b.VK.PhotosSaveMessagesPhoto(api.Params{
		"server": uploadResp.Server,
		"photo":  uploadResp.Photo,
		"hash":   uploadResp.Hash,
	})
	if err != nil {
		return "", fmt.Errorf("save photo: %w", err)
	}
	if len(photos) == 0 {
		return "", fmt.Errorf("no photos returned from save")
	}

	att := fmt.Sprintf("photo%d_%d", photos[0].OwnerID, photos[0].ID)
	if photos[0].AccessKey != "" {
		att += "_" + photos[0].AccessKey
	}
	return att, nil
}

// SendPhotoBytes sends a message with a photo from raw bytes, text, and optional keyboard.
func (b *Bot) SendPhotoBytes(peerID int, data []byte, filename, text string, keyboard *object.MessagesKeyboard) (int, error) {
	att, err := b.UploadPhotoBytes(peerID, data, filename)
	if err != nil {
		log.Printf("upload photo bytes failed: %v, sending text only", err)
		return b.SendMessage(peerID, text, keyboard)
	}

	params := api.Params{
		"peer_id":    peerID,
		"message":    text,
		"attachment": att,
		"random_id":  randomID(),
	}
	if keyboard != nil {
		kb, _ := json.Marshal(keyboard)
		params["keyboard"] = string(kb)
	}
	msgID, err := b.VK.MessagesSend(params)
	return msgID, err
}

// SendPhoto sends a message with a photo attachment, text, and optional keyboard.
func (b *Bot) SendPhoto(peerID int, photoPath, text string, keyboard *object.MessagesKeyboard) (int, error) {
	att, err := b.UploadPhoto(peerID, photoPath)
	if err != nil {
		log.Printf("upload photo failed: %v, sending text only", err)
		return b.SendMessage(peerID, text, keyboard)
	}

	params := api.Params{
		"peer_id":    peerID,
		"message":    text,
		"attachment": att,
		"random_id":  randomID(),
	}
	if keyboard != nil {
		kb, _ := json.Marshal(keyboard)
		params["keyboard"] = string(kb)
	}
	msgID, err := b.VK.MessagesSend(params)
	return msgID, err
}

// SendDoc sends a message with a document attachment, text, and optional keyboard.
func (b *Bot) SendDoc(peerID int, docPath, text string, keyboard *object.MessagesKeyboard) (int, error) {
	att, err := b.UploadDoc(peerID, docPath)
	if err != nil {
		log.Printf("upload doc failed: %v, sending text only", err)
		return b.SendMessage(peerID, text, keyboard)
	}

	params := api.Params{
		"peer_id":    peerID,
		"message":    text,
		"attachment": att,
		"random_id":  randomID(),
	}
	if keyboard != nil {
		kb, _ := json.Marshal(keyboard)
		params["keyboard"] = string(kb)
	}
	msgID, err := b.VK.MessagesSend(params)
	return msgID, err
}

// IsGroupMember checks if a user is a member of the bot's group.
func (b *Bot) IsGroupMember(userID int) (bool, error) {
	resp, err := b.VK.GroupsIsMember(api.Params{
		"group_id": b.GroupID,
		"user_id":  userID,
	})
	if err != nil {
		return false, err
	}
	return resp == 1, nil
}

// IsAnimationPath checks if a file is a .mp4 video.
func IsAnimationPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".mp4")
}

// SendMedia sends either a photo or document based on file extension.
func (b *Bot) SendMedia(peerID int, mediaPath, text string, keyboard *object.MessagesKeyboard) (int, error) {
	if IsAnimationPath(mediaPath) {
		return b.SendDoc(peerID, mediaPath, text, keyboard)
	}
	return b.SendPhoto(peerID, mediaPath, text, keyboard)
}
