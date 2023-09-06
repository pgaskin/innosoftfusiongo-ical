package fusiongo

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

type Schedule struct {
	Updated    time.Time
	Activities []ActivityInstance
	Categories []ActivityCategory
}

type ActivityInstance struct {
	Time        DateTimeRange
	Activity    string
	ActivityID  string
	Location    string
	Description string
	IsCancelled bool
}

type ActivityCategory struct {
	Category   []string
	CategoryID []string
}

// FetchSchedule fetches the latest notifications for the provided instance
// using the default CMS and HTTP client.
func FetchSchedule(ctx context.Context, schoolID int) (*Schedule, error) {
	return ProductionCMS.FetchSchedule(ctx, nil, schoolID)
}

// FetchSchedule fetches the latest notifications for the provided instance.
func (c CMS) FetchSchedule(ctx context.Context, cl *http.Client, schoolID int) (*Schedule, error) {
	fusionJSON, err := ProductionCMS.FetchJSON(ctx, nil, schoolID, "schedule")
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}

	schedule, err := ParseSchedule(fusionJSON)
	if err != nil {
		return nil, fmt.Errorf("parse: %w", err)
	}

	return schedule, nil
}

// ParseSchedule parses an Innosoft Fusion Go schedule.json, returning the
// unique activity instances and the categories they appear in, along with the
// time the schedule was last updated. The original file order is preserved.
func ParseSchedule(fusionJSON []byte) (*Schedule, error) {
	if !gjson.ValidBytes(fusionJSON) {
		return nil, fmt.Errorf("invalid JSON")
	}
	var (
		err           error
		schedule      Schedule
		scheduleIndex = map[ActivityInstance]int{} // to quickly check for dupliates
	)
	gjson.ParseBytes(fusionJSON).ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "lastUpdateUtcDateTime":
			if schedule.Updated, err = time.ParseInLocation(time.RFC3339Nano, value.Str, time.UTC); err != nil {
				err = fmt.Errorf("invalid schedule update time: %w", err)
				return false
			}
		case "categories":
			if value.IsArray() {
				value.ForEach(func(_, value gjson.Result) bool {
					if !value.IsObject() {
						err = fmt.Errorf("category array element is not an object")
						return false
					}
					var (
						categoryName       string
						categoryID         string
						categoryActivities []ActivityInstance
						categoryStart      = len(schedule.Activities) // to ensure each category is only added once per activity
					)
					value.ForEach(func(key, value gjson.Result) bool {
						switch key.Str {
						case "category":
							categoryName = value.Str
						case "id":
							categoryID = value.Str
						case "days":
							if value.IsArray() {
								value.ForEach(func(_, value gjson.Result) bool {
									if !value.IsObject() {
										err = fmt.Errorf("day array element is not an object")
										return false
									}
									var (
										dayDate  Date
										dayStart = len(categoryActivities) // to update the dates for only the events for that day
									)
									value.ForEach(func(key, value gjson.Result) bool {
										switch key.Str {
										case "date":
											var ok bool
											if dayDate, ok = ParseDate(value.Str); !ok {
												err = fmt.Errorf("invalid day date value %#v", value.Value())
												return false
											}
										case "scheduled_activities":
											if value.IsArray() {
												value.ForEach(func(_, value gjson.Result) bool {
													if !value.IsObject() {
														err = fmt.Errorf("day activities array element is not an object")
														return false
													}
													var activity ActivityInstance
													activity, err = parseActivityInstance(value)
													categoryActivities = append(categoryActivities, activity)
													return true
												})
												if err != nil {
													return false
												}
											}
										}
										return true
									})
									if err != nil {
										return false
									}
									if dayDate == (Date{}) {
										err = fmt.Errorf("missing day date")
										return false
									}
									if len(categoryActivities) > dayStart {
										for i := dayStart; i < len(categoryActivities); i++ {
											categoryActivities[i].Time.Date = dayDate
										}
									}
									return true
								})
								if err != nil {
									return false
								}
							}
						}
						return true
					})
					if err == nil {
						switch {
						case categoryName == "":
							err = fmt.Errorf("missing or empty category name")
						case categoryID == "":
							err = fmt.Errorf("missing or empty category id")
						}
					}
					if err != nil {
						return false
					}
					for _, activity := range categoryActivities {
						if i, exists := scheduleIndex[activity]; exists {
							if i < categoryStart {
								schedule.Categories[i].Category = append(schedule.Categories[i].Category, categoryName)
								schedule.Categories[i].CategoryID = append(schedule.Categories[i].CategoryID, categoryID)
							}
						} else {
							scheduleIndex[activity] = len(schedule.Activities)
							schedule.Activities = append(schedule.Activities, activity)
							schedule.Categories = append(schedule.Categories, ActivityCategory{
								Category:   []string{categoryName},
								CategoryID: []string{categoryID},
							})
						}
					}
					return true
				})
				if err != nil {
					return false
				}
			}
		}
		return true
	})
	if err == nil {
		switch {
		case schedule.Updated.IsZero():
			err = fmt.Errorf("missing schedule update time")
		}
	}
	return &schedule, err
}

func parseActivityInstance(value gjson.Result) (activity ActivityInstance, err error) {
	if !value.IsObject() {
		err = fmt.Errorf("day activities array element is not an object")
		return
	}
	var (
		hasStartTime bool
		hasEndTime   bool
	)
	value.ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "activity":
			activity.Activity = value.Str
		case "location":
			activity.Location = value.Str
		case "startTime":
			if activity.Time.Start, hasStartTime = ParseTime(value.Str); !hasStartTime {
				err = fmt.Errorf("invalid activity start time %#v", value.Value())
				return false
			}
		case "endTime":
			if activity.Time.End, hasEndTime = ParseTime(value.Str); !hasEndTime {
				err = fmt.Errorf("invalid activity start time %#v", value.Value())
				return false
			}
		case "description":
			activity.Description = strings.TrimSpace(value.Str)
		case "isCancelled":
			activity.IsCancelled = value.Bool() // truthy, not strict bool
		case "activityID":
			activity.ActivityID = value.Str
		}
		return true
	})
	if err == nil {
		switch {
		case activity.Activity == "":
			err = fmt.Errorf("activity missing activity title")
		case activity.ActivityID == "":
			err = fmt.Errorf("activity missing activity id")
		case !hasStartTime:
			err = fmt.Errorf("activity missing start time")
		case !hasEndTime:
			err = fmt.Errorf("activity missing end time")
		}
	}
	return
}
