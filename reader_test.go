// Copyright 2021 Eurac Research. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package toa5

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const format = "2006-01-02 15:04"

var in = `TOA5,Station,CR1000,S11,CR1000.Std.32.03,CPU:T1.CR1,4242,Table
TIMESTAMP,RECORD,Batt_V_Avg,,,,,
TS,RN,Volts,,,,,
,,Avg,,,,,
2020-06-07 23:45,0,12.52,,,,,
2020-06-08 00:00,1,12.56,,,,,
`

func TestEnvironment(t *testing.T) {
	tests := map[string]struct {
		in   string
		want *Environment
		rerr error
	}{
		"empty":         {in: "", want: nil, rerr: io.EOF},
		"wrongFileType": {in: "TOA3,Station,CR1000,S11,CR1000.Std.32.03,CPU:T1.CR1,4242,Table", want: nil, rerr: ErrNoTOA5File},
		"partial":       {in: "TOA5,Station,CR1000,S11,CR1000.Std.32.03,", want: nil, rerr: ErrEnvironmentLength},
		"ok": {
			in: in,
			want: &Environment{
				Filetype:  "TOA5",
				Station:   "Station",
				Model:     "CR1000",
				Serial:    "S11",
				OSVersion: "CR1000.Std.32.03",
				Program:   "CPU:T1.CR1",
				Signature: "4242",
				Table:     "Table",
			},
			rerr: nil,
		},
	}

	for k, tt := range tests {
		t.Run(k, func(t *testing.T) {
			r, err := NewReader(strings.NewReader(tt.in))
			if err != tt.rerr {
				t.Fatalf("expected error: %v. got: %v", tt.rerr, err)
			}

			if err != nil {
				return
			}

			got := r.Environment()
			diff := cmp.Diff(tt.want, got)
			if diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestFields(t *testing.T) {
	tests := map[string]struct {
		in   string
		want []string
		rerr error
	}{
		"empty": {in: "", want: nil, rerr: io.EOF},
		"ok": {
			in:   in,
			want: []string{"TIMESTAMP", "RECORD", "Batt_V_Avg", "", "", "", "", ""},
			rerr: nil,
		},
	}

	for k, tt := range tests {
		t.Run(k, func(t *testing.T) {
			r, err := NewReader(strings.NewReader(tt.in))
			if err != tt.rerr {
				t.Fatalf("expected error: %v. got: %v", tt.rerr, err)
			}

			if err != nil {
				return
			}

			got := r.Fields()
			diff := cmp.Diff(tt.want, got)
			if diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestUnits(t *testing.T) {
	tests := map[string]struct {
		in   string
		want []string
		rerr error
	}{
		"empty": {in: "", want: nil, rerr: io.EOF},
		"ok": {
			in:   in,
			want: []string{"TS", "RN", "Volts", "", "", "", "", ""},
			rerr: nil,
		},
	}

	for k, tt := range tests {
		t.Run(k, func(t *testing.T) {
			r, err := NewReader(strings.NewReader(tt.in))
			if err != tt.rerr {
				t.Fatalf("expected error: %v. got: %v", tt.rerr, err)
			}

			if err != nil {
				return
			}

			got := r.Units()
			diff := cmp.Diff(tt.want, got)
			if diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAggregation(t *testing.T) {
	tests := map[string]struct {
		in   string
		want []string
		rerr error
	}{
		"empty": {in: "", want: nil, rerr: io.EOF},
		"ok": {
			in:   in,
			want: []string{"", "", "Avg", "", "", "", "", ""},
			rerr: nil,
		},
	}

	for k, tt := range tests {
		t.Run(k, func(t *testing.T) {
			r, err := NewReader(strings.NewReader(tt.in))
			if err != tt.rerr {
				t.Fatalf("expected error: %v. got: %v", tt.rerr, err)
			}

			if err != nil {
				return
			}

			got := r.Aggregation()
			diff := cmp.Diff(tt.want, got)
			if diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestRead(t *testing.T) {
	r, err := NewReader(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		want *Record
	}{
		{&Record{
			Timestamp:   parseTime(t, format, "2020-06-07 23:45"),
			Value:       0.0,
			Name:        "RECORD",
			Unit:        "RN",
			Aggregation: "",
		}},
		{&Record{
			Timestamp:   parseTime(t, format, "2020-06-07 23:45"),
			Value:       12.52,
			Name:        "Batt_V_Avg",
			Unit:        "Volts",
			Aggregation: "Avg",
		}},
		{nil},
		{nil},
		{nil},
		{nil},
		{nil},
		{&Record{
			Timestamp:   parseTime(t, format, "2020-06-08 00:00"),
			Value:       1.0,
			Name:        "RECORD",
			Unit:        "RN",
			Aggregation: "",
		}},
		{&Record{
			Timestamp:   parseTime(t, format, "2020-06-08 00:00"),
			Value:       12.56,
			Name:        "Batt_V_Avg",
			Unit:        "Volts",
			Aggregation: "Avg",
		}},

		{nil},
		{nil},
		{nil},
		{nil},
		{nil},
	}

	for i, tt := range tests {
		got, err := r.Read()
		if errors.Is(err, ErrEmptyRecord) {
			err = nil
		}
		if err != nil {
			t.Fatal(err)
		}

		diff := cmp.Diff(tt.want, got)
		if diff != "" {
			t.Fatalf("%d: mismatch (-want +got):\n%s", i, diff)
		}
	}

}

func parseTime(t *testing.T, format, s string) time.Time {
	t.Helper()

	ts, err := time.Parse(format, s)
	if err != nil {
		t.Fatal(err)
	}

	return ts
}
