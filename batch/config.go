package batch

import "time"

type Config struct {
	ID            string        `yaml:"id" required:"true"`
	TimeSpecified string        `yaml:"time_specified" default:""`
	TimeOffset    time.Duration `yaml:"time_offset" default:"0"`
	Event         string        `yaml:"event" default:"immediately"`
	RunAt         string        `yaml:"run_at" default:"run_at"`
	StartFrom     string        `yaml:"start_from" default:""`
	StopUntil     string        `yaml:"stop_until" default:""`
	RepeatTimes   int           `yaml:"repeat_times" default:"-1"`
	Interval      time.Duration `yaml:"interval" default:"-1"`
	OnMinute      string        `yaml:"on_minute" default:""`
	OnHour        string        `yaml:"on_hour" default:""`
	OnDay         string        `yaml:"on_day" default:""`
	OnMonth       string        `yaml:"on_month" default:""`
	OnWeekday     string        `yaml:"on_weekday" default:""`
	Tasks         []string      `yaml:"tasks"`
}
