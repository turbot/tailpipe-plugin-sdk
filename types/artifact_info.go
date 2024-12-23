package types

import (
	"log/slog"
	"strconv"
	"time"

	"github.com/turbot/tailpipe-plugin-sdk/constants"
	"github.com/turbot/tailpipe-plugin-sdk/schema"
)

type ArtifactInfo struct {
	Name         string `json:"-"`
	OriginalName string `json:"-"`

	SourceEnrichment *schema.SourceEnrichment `json:"-"`

	// collection state properties
	Index     string    `json:"index,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
	// TODO do we even need to store these
	Properties map[string]string `json:"-"`
	// original properties - used to validate the granularity
	originalProperties map[string]string
}

func NewArtifactInfo(path string, opts ...ArtifactInfoOpts) *ArtifactInfo {
	res := &ArtifactInfo{
		Name:               path,
		OriginalName:       path,
		Properties:         make(map[string]string),
		originalProperties: make(map[string]string),
	}

	for _, opt := range opts {
		opt(res)
	}
	return res
}

func (i *ArtifactInfo) GetOriginalProperties() map[string]string {
	return i.originalProperties
}

// SetPathProperties sets the properties of the artifact which havbe been determined based on the path
func (i *ArtifactInfo) SetPathProperties(properties map[string]string) error {
	i.originalProperties = properties

	var year, month, day, hour, minute, second int
	var err error

	for k, v := range properties {
		switch k {
		case constants.TemplateFieldIndex:
			i.Index = v
		case constants.TemplateFieldYear:
			if year, err = strconv.Atoi(v); err != nil {
				slog.Error("error parsing year %s: %v", v, err)
				return err
			}
		case constants.TemplateFieldMonth:
			if month, err = strconv.Atoi(v); err != nil {
				slog.Error("error parsing month %s: %v", v, err)
				return err
			}
		case constants.TemplateFieldDay:
			if day, err = strconv.Atoi(v); err != nil {
				slog.Error("error parsing day %s: %v", v, err)
				return err
			}
		case constants.TemplateFieldHour:
			if hour, err = strconv.Atoi(v); err != nil {
				slog.Error("error parsing hour %s: %v", v, err)
				return err
			}
		case constants.TemplateFieldMinute:
			if minute, err = strconv.Atoi(v); err != nil {
				slog.Error("error parsing minute %s: %v", v, err)
				return err
			}
		case constants.TemplateFieldSecond:
			if second, err = strconv.Atoi(v); err != nil {
				slog.Error("error parsing second %s: %v", v, err)
				return err
			}
		default:
			i.Properties[k] = v
		}
	}

	// build timestamp from the properties provided
	// TODO #design what if not all were provided
	i.Timestamp = time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)

	return nil
}
