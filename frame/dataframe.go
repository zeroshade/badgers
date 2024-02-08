// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package frame

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/csv"
	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/pqarrow"
	"github.com/apache/badgers"
	"github.com/apache/badgers/chunked"
	"github.com/apache/badgers/series"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

func New(cols ...*series.Series) (*DataFrame, error) {
	if len(cols) == 0 {
		return &DataFrame{}, nil
	}

	if len(cols) == 1 {
		return &DataFrame{
			cols:       []*series.Series{cols[0]},
			colsByName: map[string]*series.Series{cols[0].Name(): cols[0]},
			ncols:      1,
			nrows:      cols[0].Len(),
		}, nil
	}

	names := map[string]*series.Series{cols[0].Name(): cols[0]}

	firstLen := cols[0].Len()
	for _, c := range cols[1:] {
		if c.Len() != firstLen {
			return nil, fmt.Errorf("shape error")
		}

		if _, ok := names[c.Name()]; ok {
			return nil, fmt.Errorf("duplicate name")
		}

		names[c.Name()] = c
	}

	return &DataFrame{
		cols:       cols,
		colsByName: names,
		ncols:      uint64(len(cols)),
		nrows:      uint64(firstLen),
	}, nil
}

func NewNoChecks(cols ...*series.Series) *DataFrame {
	names := map[string]*series.Series{}
	for _, c := range cols {
		names[c.Name()] = c
	}

	nrows := uint64(0)
	if len(cols) > 0 {
		nrows = cols[0].Len()
	}

	return &DataFrame{
		cols:       cols,
		ncols:      uint64(len(cols)),
		nrows:      nrows,
		colsByName: names,
	}
}

func FromTable(tbl arrow.Table) *DataFrame {
	df := &DataFrame{
		cols:       make([]*series.Series, 0, tbl.NumCols()),
		colsByName: make(map[string]*series.Series),
		ncols:      uint64(tbl.NumCols()),
		nrows:      uint64(tbl.NumRows()),
	}

	for i := 0; i < int(tbl.NumCols()); i++ {
		col := tbl.Column(i)
		s := series.FromArrowChunked(col.Name(), col.Data())
		df.cols = append(df.cols, s)
		df.colsByName[col.Name()] = s
	}

	return df
}

func FromParquet(ctx context.Context, r parquet.ReaderAtSeeker, mem badgers.Allocator) (*DataFrame, error) {
	tbl, err := pqarrow.ReadTable(ctx, r, parquet.NewReaderProperties(mem),
		pqarrow.ArrowReadProperties{Parallel: true}, mem)
	if err != nil {
		return nil, err
	}
	defer tbl.Release()
	return FromTable(tbl), nil
}

func FromCsv(r io.Reader) (*DataFrame, error) {
	rdr := csv.NewInferringReader(r, csv.WithChunk(500000),
		csv.WithHeader(true), csv.WithNullReader(true, ""))
	defer rdr.Release()

	recs := make([]arrow.Record, 0)
	defer func() {
		for _, r := range recs {
			r.Release()
		}
	}()

	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		recs = append(recs, rec)
	}

	if rdr.Err() != nil {
		return nil, rdr.Err()
	}

	schema := rdr.Schema()
	tbl := array.NewTableFromRecords(schema, recs)
	defer tbl.Release()
	return FromTable(tbl), nil
}

type DataFrame struct {
	cols         []*series.Series
	colsByName   map[string]*series.Series
	ncols, nrows uint64
}

func (d *DataFrame) Release() {
	for _, c := range d.cols {
		c.Release()
	}
	d.ncols, d.nrows = 0, 0
	d.cols, d.colsByName = nil, nil
}

func (d *DataFrame) NumCols() uint64 { return d.ncols }
func (d *DataFrame) NumRows() uint64 { return d.nrows }

func (d *DataFrame) Col(name string) *series.Series {
	return d.colsByName[name]
}

func (d *DataFrame) Columns() []*series.Series { return d.cols }
func (d *DataFrame) Column(n uint) *series.Series {
	if n > uint(len(d.cols)) {
		return nil
	}

	return d.cols[n]
}

func (d *DataFrame) printSimple() string {
	t := table.NewWriter()
	row := make([]any, d.ncols)
	for i, c := range d.cols {
		hdr := fmt.Sprintf("%s\n--\n%s", c.Name(), c.Type())
		row[i] = hdr
	}

	t.AppendHeader(table.Row(row))
	for i := uint64(0); i < d.nrows; i++ {
		tblrow := table.Row{}
		for _, c := range d.cols {
			tblrow = append(tblrow, c.GetUnchecked(uint(i)))
		}
		t.AppendRow(tblrow)
	}
	table.StyleDefault.Format.Header = text.FormatDefault
	return fmt.Sprintf("shape: (%d, %d)\n", d.nrows, d.ncols) + t.Render()
}

func (d *DataFrame) printSubset() string {
	t := table.NewWriter()
	hdrrow := table.Row{}
	hdrs := func(start, end uint64) {
		for _, c := range d.cols[start:end] {
			hdrrow = append(hdrrow, fmt.Sprintf("%s\n--\n%s", c.Name(), c.Type()))
		}
	}

	if d.ncols > 10 {
		hdrs(0, 4)
		hdrrow = append(hdrrow, "...")
		hdrs(d.ncols-4, d.ncols)
	} else {
		hdrs(0, d.ncols)
	}
	t.AppendHeader(hdrrow)

	writeRow := func(row table.Row, idx uint, colStart, colEnd uint64) table.Row {
		for _, c := range d.cols[colStart:colEnd] {
			row = append(row, c.GetUnchecked(idx))
		}
		return row
	}

	if d.nrows <= 10 {
		for i := uint64(0); i < d.nrows; i++ {
			tblrow := table.Row{}
			tblrow = writeRow(tblrow, uint(i), 0, 4)
			tblrow = append(tblrow, "...")
			tblrow = writeRow(tblrow, uint(i), d.ncols-4, d.ncols)
			t.AppendRow(tblrow)
		}
	} else {
		for i := uint64(0); i < 5; i++ {
			tblrow := table.Row{}
			if d.ncols <= 10 {
				tblrow = writeRow(tblrow, uint(i), 0, d.ncols)
			} else {
				tblrow = writeRow(tblrow, uint(i), 0, 4)
				tblrow = append(tblrow, "...")
				tblrow = writeRow(tblrow, uint(i), d.ncols-4, d.ncols)
			}
			t.AppendRow(tblrow)
		}

		sep := table.Row{}
		for range hdrrow {
			sep = append(sep, "...")
		}
		t.AppendRow(sep)

		for i := uint64(d.nrows - 5); i < d.nrows; i++ {
			tblrow := table.Row{}
			if d.ncols <= 10 {
				tblrow = writeRow(tblrow, uint(i), 0, d.ncols)
			} else {
				tblrow = writeRow(tblrow, uint(i), 0, 4)
				tblrow = append(tblrow, "...")
				tblrow = writeRow(tblrow, uint(i), d.ncols-4, d.ncols)
			}
			t.AppendRow(tblrow)
		}
	}

	table.StyleDefault.Format.Header = text.FormatDefault
	return fmt.Sprintf("shape: (%d, %d)\n", d.nrows, d.ncols) + t.Render()
}

func (d *DataFrame) String() string {
	if d.ncols <= 10 && d.nrows <= 10 {
		return d.printSimple()
	}
	return d.printSubset()
}

func (d *DataFrame) tryApplyCols(fn func(*series.Series) (*series.Series, error)) (out []*series.Series, err error) {
	out = make([]*series.Series, len(d.cols))
	defer func() {
		if err != nil {
			for _, c := range out {
				if c != nil {
					c.Release()
				}
			}
		}
	}()

	for i, c := range d.cols {
		out[i], err = fn(c)
		if err != nil {
			return nil, err
		}
	}

	return
}

func (d *DataFrame) Filter(ctx context.Context, mask *chunked.Bool) (*DataFrame, error) {
	newCols, err := d.tryApplyCols(func(s *series.Series) (*series.Series, error) {
		return s.Filter(ctx, mask)
	})

	if err != nil {
		return nil, err
	}

	return NewNoChecks(newCols...), nil
}
