package fusiongo

import (
	"context"
	"fmt"
	"time"

	"github.com/tidwall/gjson"
)

type Notifications struct {
	Updated       time.Time
	Notifications []Notification
}

type Notification struct {
	ID   string
	Text string
	Sent DateTime
}

// FetchNotifications fetches the latest notifications for the provided instance
// using the default CMS and HTTP client.
func FetchNotifications(ctx context.Context, schoolID int) (*Notifications, error) {
	return fetchAndParse(ctx, schoolID, "notifications", ParseNotifications)
}

// ParseNotifications parses an Innosoft Fusion Go notifications.json, returning
// the notifications along with the time the schedule was last updated. The
// original file order is preserved.
func ParseNotifications(fusionJSON []byte) (*Notifications, error) {
	if !gjson.ValidBytes(fusionJSON) {
		return nil, fmt.Errorf("invalid JSON")
	}
	var (
		err             error
		notifications   Notifications
		notificationIDs = map[string]int{}
	)
	gjson.ParseBytes(fusionJSON).ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "lastUpdateUtcDateTime":
			if notifications.Updated, err = time.ParseInLocation(time.RFC3339Nano, value.Str, time.UTC); err != nil {
				err = fmt.Errorf("invalid notifications update time: %w", err)
				return false
			}
		case "notifications":
			if value.IsArray() {
				value.ForEach(func(_, value gjson.Result) bool {
					if !value.IsObject() {
						err = fmt.Errorf("notification array element is not an object")
						return false
					}
					var notification Notification
					notification, err = parseNotification(value)
					if err != nil {
						return false
					}
					notifications.Notifications = append(notifications.Notifications, notification)
					return true
				})
				if err != nil {
					return false
				}
			}
			if err != nil {
				return false
			}
		}
		return true
	})
	if err == nil {
		switch {
		case notifications.Updated.IsZero():
			err = fmt.Errorf("missing schedule update time")
		default:
			for _, notification := range notifications.Notifications {
				if _, seen := notificationIDs[notification.ID]; seen {
					err = fmt.Errorf("duplicate notification id %q", notification.ID)
				}
			}
		}
	}
	return &notifications, err
}

func parseNotification(value gjson.Result) (notification Notification, err error) {
	if !value.IsObject() {
		err = fmt.Errorf("notification array element is not an object")
		return
	}
	var (
		hasNotificationSent bool
	)
	value.ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "id":
			// used to be a string, is now an int
			notification.ID = value.Str
			if notification.ID == "" {
				notification.ID = value.Raw
			}
		case "notification":
			notification.Text = value.Str
		case "datetime_sent":
			if notification.Sent, hasNotificationSent = ParseDateTime(value.Str); !hasNotificationSent {
				err = fmt.Errorf("invalid notification sent datetime %#v", value.Value())
				return false
			}
		}
		return true
	})
	if err == nil {
		if notification.ID == "" {
			err = fmt.Errorf("missing notification id")
		}
		if notification.Text == "" {
			err = fmt.Errorf("missing notification text")
		}
		if !hasNotificationSent {
			err = fmt.Errorf("missing notification sent time")
		}
	}
	return
}
