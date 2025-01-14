package point

import (
	valid "github.com/asaskevich/govalidator"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/influxdata/influx-stress/lineprotocol"
)

// The point struct implements the lineprotocol.Point interface.
type point struct {
	seriesKey []byte

	// Note here that Ints and Floats are exported so they can be modified outside
	// of the point struct
	Ints    []*lineprotocol.Int
	Floats  []*lineprotocol.Float
	Strings []*lineprotocol.String

	// The fields slice should contain exactly Ints and Floats. Having this
	// slice allows us to avoid iterating through Ints and Floats in the Fields
	// function.
	fields []lineprotocol.Field

	time *lineprotocol.Timestamp
}

// New returns a new point without setting the time field.
func New(sk []byte, ints, floats []string, p lineprotocol.Precision) *point {
	fields := []lineprotocol.Field{}
	e := &point{
		seriesKey: sk,
		time:      lineprotocol.NewTimestamp(p),
		fields:    fields,
	}

	//for _, i := range ints {
	//	n := &lineprotocol.Int{Key: []byte(i)}
	//	e.Ints = append(e.Ints, n)
	//	e.fields = append(e.fields, n)
	//}
	//
	//for _, f := range floats {
	//	n := &lineprotocol.Float{Key: []byte(f)}
	//	e.Floats = append(e.Floats, n)
	//	e.fields = append(e.fields, n)
	//}

	return e
}

// Series returns the series key for a point.
func (p *point) Series() []byte {
	return p.seriesKey
}

// Fields returns the fields for a a point.
func (p *point) Fields() []lineprotocol.Field {
	return p.fields
}

// Time returns the timestamps for a point.
func (p *point) Time() *lineprotocol.Timestamp {
	return p.time
}

// SetTime set the t to be the timestamp for a point.
func (p *point) SetTime(t time.Time) {
	p.time.SetTime(&t)
}

// Update increments the value of all of the Int and Float
// fields by 1.
func (p *point) Update() {
	for _, i := range p.Ints {
		atomic.AddInt64(&i.Value, int64(1))
	}

	for _, f := range p.Floats {
		// Need to do something else here
		// There will be a race here
		f.Value += 1.0
	}
}

// NewPoints returns a slice of Points of length seriesN shaped like the given seriesKey.
func NewPoints(seriesKey, fields string, seriesN int, pc lineprotocol.Precision) []lineprotocol.Point {
	pts := []lineprotocol.Point{}
	series := generateSeriesKeys(seriesKey, seriesN)
	ints, floats := generateFieldSet(fields)
	s := strings.Split(fields, ",")
	for _, sk := range series {
		p := New(sk, ints, floats, pc)

		for _, field := range s {
			keyValue := strings.Split(field, "=")
			key := keyValue[0]
			strValue := keyValue[1]

			if strings.HasSuffix(strValue, "i") {
				value, _ := strconv.ParseInt(strings.TrimSuffix(strValue, "i"), 10, 64)
				intField := &lineprotocol.Int{Key: []byte(key), Value: value}
				p.Ints = append(p.Ints, intField)
				p.fields = append(p.fields, intField)
			} else if valid.IsFloat(strValue) {
				value, _ := strconv.ParseFloat(strValue, 64)
				floatField := &lineprotocol.Float{Key: []byte(key), Value: value}
				p.Floats = append(p.Floats, floatField)
				p.fields = append(p.fields, floatField)
			} else {
				strValueWithQuotes := "\"" + strValue + "\""
				stringField := &lineprotocol.String{Key: []byte(key), Value: strValueWithQuotes}
				p.Strings = append(p.Strings, stringField)
				p.fields = append(p.fields, stringField)
			}
		}

		pts = append(pts, p)
	}

	return pts
}

func IsDigitsOnly(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
