package fusiongo

import (
	"context"
	"fmt"

	"github.com/tidwall/gjson"
)

type Facilities struct {
	Facilities []Facility
}

type Facility struct {
	Updated Date

	ID      string
	Name    string
	Phone   string
	Email   string
	Website string

	Address   string
	Longitude float64
	Latitude  float64

	HoursTitle string
	Hours      [7]string // [time.Weekday]string
}

// FetchFacilities fetches the latest facilities for the provided instance using
// the default CMS and HTTP client.
func FetchFacilities(ctx context.Context, schoolID int) (*Facilities, error) {
	return fetchAndParse(ctx, schoolID, "facilities", ParseFacilities)
}

// ParseFacilities parses an Innosoft Fusion Go facilities.json.
func ParseFacilities(fusionJSON []byte) (*Facilities, error) {
	if !gjson.ValidBytes(fusionJSON) {
		return nil, fmt.Errorf("invalid JSON")
	}
	var (
		err         error
		facilities  Facilities
		facilityIDs = map[string]int{}
	)
	gjson.ParseBytes(fusionJSON).ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "facilities":
			if value.IsArray() {
				value.ForEach(func(_, value gjson.Result) bool {
					if !value.IsObject() {
						err = fmt.Errorf("facility array element is not an object")
						return false
					}
					var facility Facility
					facility, err = parseFacility(value)
					if err != nil {
						return false
					}
					facilities.Facilities = append(facilities.Facilities, facility)
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
		for _, facility := range facilities.Facilities {
			if _, seen := facilityIDs[facility.ID]; seen {
				err = fmt.Errorf("duplicate facility id %q", facility.ID)
			}
		}
	}
	return &facilities, err
}

func parseFacility(value gjson.Result) (facility Facility, err error) {
	if !value.IsObject() {
		err = fmt.Errorf("facility array element is not an object")
		return
	}
	var (
		hasUpdated bool
	)
	value.ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "id":
			facility.ID = value.Str
		case "facilityname":
			facility.Name = value.Str
		case "latitude":
			facility.Latitude = value.Float()
		case "longitude":
			facility.Longitude = value.Float()
		case "title":
			facility.HoursTitle = value.Str
		case "phonecall":
			facility.Phone = value.Str
		case "email":
			facility.Email = value.Str
		case "website":
			facility.Website = value.Str
		case "hours1", "hours2", "hours3", "hours4", "hours5", "hours6", "hours7":
			facility.Hours[key.Str[len("hours")]-'1'] = value.Str
		case "updated":
			if facility.Updated, hasUpdated = ParseDate(value.Str); !hasUpdated {
				err = fmt.Errorf("invalid facility updated datetime %#v", value.Value())
				return false
			}
		}
		return true
	})
	if err == nil {
		switch {
		case facility.ID == "":
			err = fmt.Errorf("missing facility id")
		case facility.Name == "":
			err = fmt.Errorf("missing facility name")
		case !hasUpdated:
			err = fmt.Errorf("missing facility update time")
		}
	}
	return
}
