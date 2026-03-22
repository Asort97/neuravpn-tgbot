# Промпт для ТГ бота: привязка аккаунта ВК

## Контекст

У нас два бота (TG и VK), которые работают с ОДНОЙ базой данных PostgreSQL.
- TG-пользователи хранятся как `"623290294"` (чистый числовой ID)
- VK-пользователи хранятся как `"vk_123456"` (с префиксом)

Реализована механика **linked_to**: после привязки VK-юзер становится "алиасом" TG-юзера.
В БД у VK-юзера колонка `linked_to` указывает на TG user ID.
Все операции VK-бота (дни, email, Xray, оплата) проходят через `resolvedUserID()`,
который возвращает TG user ID вместо VK — т.е. оба бота работают с ОДНОЙ записью.

## Что уже готово (VK-бот, файл main.go)

1. В postgres.go / sqlite.go есть: `SetLinkedTo(userID, linkedTo)`, `GetLinkedTo(userID)`, `SetLinkToken(userID, token)`, `GetUserByLinkToken(token)`, `ClearLinkToken(userID)`
2. Колонки `link_token TEXT` и `linked_to TEXT` добавляются в ALTER TABLE автоматически
3. VK-бот:
   - `resolvedUserID(peerID)` — разрешает VK peer → TG user ID если привязан (с кешем)
   - Все хендлеры используют `resolvedUserID()` вместо `vkUserIDStr()`
   - `handleLinkAccount(token)` — VK-юзер отправляет `link_XXXX`, бот:
     1. Ищет TG-юзера по токену через `GetUserByLinkToken`
     2. Переносит VK-дни в TG-юзера через `AddDays`
     3. Обнуляет VK-дни
     4. Ставит `SetLinkedTo(vkUserID, tgUserID)`
     5. Очищает токен `ClearLinkToken`
     6. Обновляет кеш

## Что нужно сделать в ТГ-боте

### 1. Кнопка «🔗 связать с ВК» в профиле

В `handleStatus` (или аналогичном хендлере профиля) добавить inline-кнопку:
```
🔗 связать с ВК  →  callback_data: "link_vk"
```

### 2. Хендлер `handleLinkVK` (по callback "link_vk")

```
func handleLinkVK(chatID int64):
    userID := strconv.FormatInt(chatID, 10)
    token := generateRandomToken(16)   // 16 символов, a-zA-Z0-9
    userStore.SetLinkToken(userID, token)

    text := "🔗 привязка ВК\n\n" +
        "отправьте это сообщение нашему VK-боту:\n\n" +
        "link_" + token + "\n\n" +
        "VK-бот: https://vk.com/neuravpn\n\n" +
        "⚠️ токен одноразовый."
    
    // Отправить с inline кнопкой "Перейти в ВК бота"
    keyboard := InlineButton("🔗 перейти в ВК", url="https://vk.com/neuravpn")
```

### 3. Показать статус привязки в профиле

В `handleStatus` после заголовка профиля, проверить:
```go
// Проверить, привязан ли какой-то VK-аккаунт к этому TG-юзеру
// Можно сделать SELECT id FROM users WHERE linked_to = $1 AND id LIKE 'vk_%'
// Если найден — показать "• ВК: привязан (vk_12345)"
```

Для этого может понадобиться добавить новый метод в DataStore:
```go
GetLinkedVKUser(tgUserID string) (string, error)
// SELECT id FROM users WHERE linked_to = $1 AND id LIKE 'vk_%' LIMIT 1
```

### 4. Интерфейс DataStore в ТГ-боте

Добавить в интерфейс DataStore (если ещё нет):
```go
SetLinkToken(userID, token string) error
GetUserByLinkToken(token string) (string, error)
ClearLinkToken(userID string) error
SetLinkedTo(userID, linkedTo string) error
GetLinkedTo(userID string) (string, error)
```

### Важно

- Кнопку надо показывать ТОЛЬКО если аккаунт ещё НЕ привязан к ВК
- Если уже привязан → показать виджет "привязан к vk_XXX" без кнопки
- НЕ нужен `resolvedUserID` на стороне TG бота — TG-юзер всегда использует свой собственный ID, это VK-юзер ссылается на TG
- Токен должен быть case-insensitive при сравнении (VK бот уже делает trim/lower)

## Схема потока

```
TG бот:                          VK бот:
1. Юзер нажал "связать с ВК"
2. SetLinkToken(tgID, token)
3. Показал: "отправь link_XXX в ВК"
4. Юзер копирует токен           5. Юзер отправляет "link_XXX"  
                                  6. GetUserByLinkToken(XXX) → tgID
                                  7. AddDays(tgID, vkDays)
                                  8. SetLinkedTo(vkID, tgID)
                                  9. ClearLinkToken(tgID)
                                  10. Все будущие операции VK → через tgID
```
