package retention

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	_ "net/http/pprof"

	pcli "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	"github.com/pilosa/pilosa"
)

/***********************
use case implementation
***********************/

// TODO autoscan 1. determine field type by attempting conversions
// TODO autoscan 2. determine field mapping by looking at statistics (for floatmapper, intmapper)
// TODO autoscan 3. write results from ^^ to config file
// TODO read ParserMapper config from file (cant do CustomMapper)

type Main struct {
	PilosaHost  string
	URLFile     string
	Concurrency int
	Index       string
	BufferSize  int

	importer pdk.PilosaImporter
	urls     []string

	recordMapper []pdk.BitMapper

	channelIDs             *StringIDs
	countryIDs             *StringIDs
	osIDs                  *StringIDs
	distributionIDs        *StringIDs
	appVersionIDs          *StringIDs
	localeIDs              *StringIDs
	defaultSearchEngineIDs *StringIDs

	nexter *Nexter

	totalBytes int64
	bytesLock  sync.Mutex

	totalRecs   *Counter
	skippedRecs *Counter
}

func NewMain() *Main {
	m := &Main{
		Concurrency: 1,
		nexter:      &Nexter{},
		urls:        make([]string, 0),

		totalRecs:   &Counter{},
		skippedRecs: &Counter{},

		channelIDs: NewStringIDs(),
		countryIDs: NewStringIDs(),
		osIDs:      NewStringIDs(),
		// distributionIDs:        NewStringIDs(),
		// appVersionIDs:          NewStringIDs(),
		// localeIDs:              NewStringIDs(),
		// defaultSearchEngineIDs: NewStringIDs(),
	}

	return m
}

func (m *Main) Run() error {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := m.readURLs()
	if err != nil {
		return err
	}

	frames := []string{
		"channel",
		"country",
		"os",
		"profile_creation_date",
		"subsession_start_date",
		/*
			"subsession_length",
			"distribution_id",
			"submission_date",
			"sync_configured",
			"app_version",
			"locale",
			"active_addons_count",
			"is_default_browser",
			"default_search_engine",
			"unique_domains_count",
			"total_uri_count" */
	}

	m.importer = pdk.NewImportClient(m.PilosaHost, m.Index, frames, m.BufferSize)

	pilosaURI, err := pcli.NewURIFromAddress(m.PilosaHost)
	if err != nil {
		return fmt.Errorf("interpreting pilosaHost '%v': %v", m.PilosaHost, err)
	}
	setupClient := pcli.NewClientWithURI(pilosaURI)
	index, err := pcli.NewIndex(m.Index, &pcli.IndexOptions{})
	if err != nil {
		return fmt.Errorf("making index: %v", err)
	}
	err = setupClient.EnsureIndex(index)
	if err != nil {
		return fmt.Errorf("ensuring index existence: %v", err)
	}
	for _, frame := range frames {
		fram, err := index.Frame(frame, &pcli.FrameOptions{CacheType: pilosa.CacheTypeRanked})
		if err != nil {
			return fmt.Errorf("making frame: %v", err)
		}
		err = setupClient.EnsureFrame(fram)
		if err != nil {
			return fmt.Errorf("creating frame '%v': %v", frame, err)
		}
	}

	ticker := m.printStats()

	urls := make(chan string)
	records := make(chan Record)

	go func() {
		for _, url := range m.urls {
			urls <- url
		}
		close(urls)
	}()

	m.greenBms = getBitMappers(greenFields)
	m.yellowBms = getBitMappers(yellowFields)
	m.ams = getAttrMappers()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Profiles: %d, Bytes: %s", m.nexter.Last(), pdk.Bytes(m.BytesProcessed()))
			os.Exit(0)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg.Add(1)
		go func() {
			m.fetch(urls, records)
			wg.Done()
		}()
	}
	var wg2 sync.WaitGroup
	for i := 0; i < m.Concurrency; i++ {
		wg2.Add(1)
		go func() {
			m.parseMapAndPost(records)
			wg2.Done()
		}()
	}
	wg.Wait()
	close(records)
	wg2.Wait()
	m.importer.Close()
	ticker.Stop()
	return err
}

func (m *Main) readURLs() error {
	if m.URLFile == "" {
		return fmt.Errorf("Need to specify a URL File")
	}
	f, err := os.Open(m.URLFile)
	if err != nil {
		return err
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		m.urls = append(m.urls, s.Text())
	}
	if err := s.Err(); err != nil {
		return err
	}
	return nil
}

func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.BytesProcessed()
			log.Printf("Profiles: %d, Bytes: %s, Records: %v, Duration: %v, Rate: %v/s",
				m.nexter.Last(), pdk.Bytes(bytes), m.totalRecs.Get(),
				duration, pdk.Bytes(float64(bytes)/duration.Seconds()))
			log.Printf("Skipped: %v", m.skippedRecs.Get())
		}
	}()
	return t
}

func (m *Main) fetch(urls <-chan string, records chan<- Record) {
	for url := range urls {
		var content io.ReadCloser
		if strings.HasPrefix(url, "http") {
			resp, err := http.Get(url)
			if err != nil {
				log.Printf("fetching %s, err: %v", url, err)
				continue
			}
			content = resp.Body
		} else {
			f, err := os.Open(url)
			if err != nil {
				log.Printf("opening %s, err: %v", url, err)
				continue
			}
			content = f
		}

		scan := bufio.NewScanner(content)
		// discard header line
		correctLine := false
		if scan.Scan() {
			header := scan.Text()
			if strings.HasPrefix(header, "client_id") {
				correctLine = true
			}
		}
		for scan.Scan() {
			m.totalRecs.Add(1)
			record := scan.Text()
			m.AddBytes(len(record))
			if correctLine {
				// last field needs to be shifted over by 1
				lastcomma := strings.LastIndex(record, ",")
				if lastcomma == -1 {
					m.skippedRecs.Add(1)
					continue
				}
				record = record[:lastcomma] + "," + record[lastcomma:]
			}
			records <- Record{Val: record}
		}
		err := content.Close()
		if err != nil {
			log.Printf("closing %s, err: %v", url, err)
		}
	}
}

type Record struct {
	Val string
}

func (r Record) Clean() ([]string, bool) {
	if len(r.Val) == 0 {
		return nil, false
	}
	fields := strings.Split(r.Val, ",")
	return fields, true
}

type BitFrame struct {
	Bit   uint64
	Frame string
}

func (m *Main) parseMapAndPost(records <-chan Record) {
Records:
	for record := range records {
		fields, ok := record.Clean()
		if !ok {
			m.skippedRecs.Add(1)
			continue
		}

		var bms []pdk.BitMapper
		bitsToSet := make([]BitFrame, 0)

		for _, bm := range bms {
			if len(bm.Fields) != len(bm.Parsers) {
				// TODO if len(pm.Parsers) == 1, use that for all fields
				log.Fatalf("parse: BitMapper has different number of fields: %v and parsers: %v", bm.Fields, bm.Parsers)
			}

			// parse fields into a slice `parsed`
			parsed := make([]interface{}, 0, len(bm.Fields))
			for n, fieldnum := range bm.Fields {
				parser := bm.Parsers[n]
				if fieldnum >= len(fields) {
					log.Printf("parse: field index: %v out of range for: %v", fieldnum, fields)
					m.skippedRecs.Add(1)
					continue Records
				}
				parsedField, err := parser.Parse(fields[fieldnum])
				if err != nil && fields[fieldnum] == "" {
					m.skippedRecs.Add(1)
					continue Records
				} else if err != nil {
					log.Printf("parsing: field: %v err: %v bm: %v rec: %v", fields[fieldnum], err, bm, record)
					m.skippedRecs.Add(1)
					continue Records
				}
				parsed = append(parsed, parsedField)
			}

			// Handle errors during the parsing process
			ids, err := bm.Mapper.ID(parsed...)
			if err != nil {
				log.Printf("mapping: bm: %v, err: %v rec: %v", bm, err, record)
				m.skippedRecs.Add(1)
				continue Records
			}

			// map those fields to a slice of IDs
			for _, id := range ids {
				bitsToSet = append(bitsToSet, BitFrame{Bit: uint64(id), Frame: bm.Frame})
			}
		}
		profileID := m.nexter.Next()
		for _, bit := range bitsToSet {
			m.importer.SetBit(bit.Bit, profileID, bit.Frame)
		}
	}
}

func getBitMappers(fields map[string]int) []pdk.BitMapper {
	tp := pdk.TimeParser{Layout: "2006-01-02 15:04:05"}

	// TODO: Define a custom mapper for categorical attributes. This takes advantage of the StringIDs mapper. This should
	// first generate an integer id, and then apply the IntMapper on top of this

	// TODO: Create an array of mappers that correspond to each column in the csv file.
	// 	"channel",
	//	"country",
	//	"os",
	//	"profile_creation_date",
	//	"subsession_start_date",
	// The above colums are the attributes that will be focused on in the first iteration.

	recordMapper := []pdk.BitMapper{
		pdk.BitMapper{
			Frame:   "passenger_count",
			Mapper:  pdk.IntMapper{Min: 0, Max: 9},
			Parsers: []pdk.Parser{pdk.IntParser{}},
			Fields:  []int{fields["passenger_count"]},
		},
		pdk.BitMapper{
			Frame:   "pickup_day",
			Mapper:  pdk.DayOfWeekMapper{},
			Parsers: []pdk.Parser{tp},
			Fields:  []int{fields["pickup_datetime"]},
		},
	}

	return recordMapper
}

func (m *Main) AddBytes(n int) {
	m.bytesLock.Lock()
	m.totalBytes += int64(n)
	m.bytesLock.Unlock()
}

func (m *Main) BytesProcessed() (num int64) {
	m.bytesLock.Lock()
	num = m.totalBytes
	m.bytesLock.Unlock()
	return
}

type Counter struct {
	num  int64
	lock sync.Mutex
}

func (c *Counter) Add(n int) {
	c.lock.Lock()
	c.num += int64(n)
	c.lock.Unlock()
}

func (c *Counter) Get() (ret int64) {
	c.lock.Lock()
	ret = c.num
	c.lock.Unlock()
	return
}

// Nexter generates unique bitmapIDs
type Nexter struct {
	id   uint64
	lock sync.Mutex
}

// Next generates a new bitmapID
func (n *Nexter) Next() (nextID uint64) {
	n.lock.Lock()
	nextID = n.id
	n.id++
	n.lock.Unlock()
	return
}

func (n *Nexter) Last() (lastID uint64) {
	n.lock.Lock()
	lastID = n.id - 1
	n.lock.Unlock()
	return
}
