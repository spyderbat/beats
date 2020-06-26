// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package spyder

import (
	"bufio"
	"fmt"
	//"golang.org/x/sys/unix"
	"os"
	"runtime"
	//"syscall"

	//"syscall"
	"time"
	//	"golang.org/x/sys/unix"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/json"
	"github.com/elastic/beats/v7/libbeat/publisher"
)

type console struct {
	log      *logp.Logger
	out      *os.File
	observer outputs.Observer
	writer   *bufio.Writer
	codec    codec.Codec
	index    string
	fifo_name string
	fifo_size int
	fifo     *os.File
}

type consoleEvent struct {
	Timestamp time.Time `json:"@timestamp" struct:"@timestamp"`

	// Note: stdlib json doesn't support inlining :( -> use `codec: 2`, to generate proper event
	Fields interface{} `struct:",inline"`
}

func init() {
	outputs.RegisterType("spyder", makeSpyder)
}

func makeSpyder(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	config := defaultConfig
	err := cfg.Unpack(&config)
	if err != nil {
		return outputs.Fail(err)
	}

	var enc codec.Codec
	if config.Codec.Namespace.IsSet() {
		enc, err = codec.CreateEncoder(beat, config.Codec)
		if err != nil {
			return outputs.Fail(err)
		}
	} else {
		enc = json.New(beat.Version, json.Config{
			Pretty:     config.Pretty,
			EscapeHTML: false,
		})
	}

	fmt.Println(config.FifoName)
	fmt.Println(config.FifoSize)
	// Make a FIFO if it doesn't exist
	if !file_exists(config.FifoName) {
		fmt.Errorf("FIFO missing! {err}")
		return outputs.Fail(err)
	}
	fmt.Println("ISN!!!! fifo is already there")
	file, err := os.OpenFile(config.FifoName, os.O_WRONLY, os.ModeNamedPipe)
	if err != nil {
		fmt.Errorf("FIFO couldn't be opened! #{err}")
		return outputs.Fail(err)
	}
	fmt.Println("After fifo stuff")

	index := beat.Beat
	c, err := newSpyder(index, observer, enc, file)
	if err != nil {
		return outputs.Fail(fmt.Errorf("console output initialization failed with: %v", err))
	}
	c.fifo = file

	// check stdout actually being available
	if runtime.GOOS != "windows" {
		if _, err = c.out.Stat(); err != nil {
			err = fmt.Errorf("console output initialization failed with: %v", err)
			return outputs.Fail(err)
		}
	}

	return outputs.Success(config.BatchSize, 0, c)
}

func file_exists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func newSpyder(index string, observer outputs.Observer, codec codec.Codec, fifo *os.File) (*console, error) {
	c := &console{log: logp.NewLogger("console"), out: fifo, codec: codec, observer: observer, index: index}
	c.writer = bufio.NewWriterSize(c.out, 8*1024)
	return c, nil
}

func (c *console) Close() error { return nil }
func (c *console) Publish(batch publisher.Batch) error {
	st := c.observer
	events := batch.Events()
	st.NewBatch(len(events))

	dropped := 0
	for i := range events {
		ok := c.publishEvent(&events[i])
		if !ok {
			dropped++
		}
	}

	c.writer.Flush()
	batch.ACK()

	st.Dropped(dropped)
	st.Acked(len(events) - dropped)

	return nil
}

var nl = []byte("\n")

func (c *console) publishEvent(event *publisher.Event) bool {
	serializedEvent, err := c.codec.Encode(c.index, &event.Content)
	if err != nil {
		if !event.Guaranteed() {
			return false
		}

		c.log.Errorf("Unable to encode event: %+v", err)
		c.log.Debugf("Failed event: %v", event)
		return false
	}

	if err := c.writeBuffer(serializedEvent); err != nil {
		c.observer.WriteError(err)
		c.log.Errorf("Unable to publish events to console: %+v", err)
		return false
	}

	if err := c.writeBuffer(nl); err != nil {
		c.observer.WriteError(err)
		c.log.Errorf("Error when appending newline to event: %+v", err)
		return false
	}

	c.observer.WriteBytes(len(serializedEvent) + 1)
	return true
}

func (c *console) writeBuffer(buf []byte) error {
	written := 0
	for written < len(buf) {
		n, err := c.writer.Write(buf[written:])
		if err != nil {
			return err
		}

		written += n
	}
	return nil
}

func (c *console) String() string {
	return "spyder"
}
