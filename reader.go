// Copyright 2021 Eurac Research. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package toa5 provides a reader for TOA5 files. The TOA5 file format is used
// as an output format by the CR1000 data loggers of Campbell Scientific.
package toa5

import (
	"encoding/csv"
	"errors"
	"io"
	"math"
	"strconv"
	"time"
)

// General errors.
var (
	ErrNoTOA5File        = errors.New("no TOA5 file")
	ErrEmptyRecord       = errors.New("empty record name")
	ErrNoOptions         = errors.New("no options provided")
	ErrEnvironmentLength = errors.New("environment line has missing fields")
)

// Record denotes a single record in a TOA5 file with its associated metadata.
type Record struct {
	Timestamp   time.Time
	Value       float64
	Name        string
	Unit        string
	Aggregation string
}

// Environment denotes the first header line in a TOA5 file.
type Environment struct {
	Filetype  string
	Station   string
	Model     string
	Serial    string
	OSVersion string
	Program   string
	Signature string
	Table     string
}

type Options struct {
	TimeLayout   string
	TimeLocation *time.Location
	Delimiter    rune
}

// Reader is a reader for TOA5 files and wraps and csv.Reader.
type Reader struct {
	r            *csv.Reader  // The underling reader.
	options      *Options     // Options for reading.
	currentRow   []string     // Buffer for to store the current read row.
	columnIndex  int          // Buffer for to store the last read field.
	rowTimestamp time.Time    // Buffer of the timestamp for the currentLine.
	environment  *Environment // The first line in a TOA5 file.
	fields       []string     // The second line in a TOA5 file.
	units        []string     // The third line in a TOA5 file.
	aggregation  []string     // The fourth line in a TOA5 file.
}

// NewReader will return a new TOA5 reader.
func NewReader(in io.Reader) (*Reader, error) {
	return newReader(in, &Options{
		Delimiter: ',',
	})
}

func NewReaderWithOptions(in io.Reader, opt *Options) (*Reader, error) {
	if opt == nil {
		return nil, ErrNoOptions
	}

	return newReader(in, opt)
}

func newReader(in io.Reader, opt *Options) (*Reader, error) {
	if opt.TimeLocation == nil {
		opt.TimeLocation = time.UTC
	}

	if opt.TimeLayout == "" {
		opt.TimeLayout = "2006-01-02 15:04:05"
	}

	csvr := csv.NewReader(in)
	csvr.Comma = opt.Delimiter
	csvr.TrimLeadingSpace = true

	r := &Reader{
		r:       csvr,
		options: opt,
	}

	if err := r.readHeader(); err != nil {
		return nil, err
	}

	if err := r.readNextRow(); err != nil {
		return nil, err
	}

	return r, nil
}

// Environment returns the environment header line of the TOA5.
func (r *Reader) Environment() *Environment { return r.environment }

// Fields returns the field header line of the TOA5.
func (r *Reader) Fields() []string { return r.fields }

// Units returns the unit header line of the TOA5.
func (r *Reader) Units() []string { return r.units }

// Aggregation returns the aggregation header line of the TOA5.
func (r *Reader) Aggregation() []string { return r.aggregation }

func (r *Reader) readHeader() error {
	err := r.readEnvironmentLine()
	if err != nil {
		return err
	}

	// read field names
	r.fields, err = r.r.Read()
	if err != nil {
		return err
	}

	// read unit line
	r.units, err = r.r.Read()
	if err != nil {
		return err
	}

	// read aggregation line
	r.aggregation, err = r.r.Read()
	if err != nil {
		return err
	}

	return nil
}

func (r *Reader) readEnvironmentLine() error {
	fields, err := r.r.Read()
	if err != nil {
		return err
	}

	if len(fields) < 8 {
		return ErrEnvironmentLength
	}

	r.environment = &Environment{
		Filetype:  fields[0],
		Station:   fields[1],
		Model:     fields[2],
		Serial:    fields[3],
		OSVersion: fields[4],
		Program:   fields[5],
		Signature: fields[6],
		Table:     fields[7],
	}

	if r.environment.Filetype != "TOA5" {
		return ErrNoTOA5File
	}

	return nil
}

func (r *Reader) readNextRow() error {
	var err error
	r.currentRow, err = r.r.Read()
	if err != nil {
		return err
	}

	// Reset columnIndex.
	r.columnIndex = 0

	r.rowTimestamp, err = time.ParseInLocation(r.options.TimeLayout, r.currentRow[0], r.options.TimeLocation)
	if err == nil {
		return nil
	}

	// We got an parse error. Try to parse the the timestamp without seconds
	const format = "2006-01-02 15:04"
	r.rowTimestamp, err = time.ParseInLocation(format, r.currentRow[0], r.options.TimeLocation)
	return err
}

// Read reads and returns a Record.
func (r *Reader) Read() (*Record, error) {
	r.columnIndex += 1

	// We have read the last column, so we need to read a new row and continue
	// with that.
	if r.columnIndex >= len(r.currentRow) {
		err := r.readNextRow()
		if err != nil {
			return nil, err
		}

		r.columnIndex += 1
	}

	// Parsing current cell to float64. If it fails declare the value as NaN.
	v, err := strconv.ParseFloat(r.currentRow[r.columnIndex], 64)
	if err != nil {
		v = math.NaN()
	}

	name := r.fields[r.columnIndex]
	if name == "" {
		return nil, ErrEmptyRecord
	}

	return &Record{
		Timestamp:   r.rowTimestamp,
		Value:       v,
		Name:        r.fields[r.columnIndex],
		Unit:        r.units[r.columnIndex],
		Aggregation: r.aggregation[r.columnIndex],
	}, nil
}
