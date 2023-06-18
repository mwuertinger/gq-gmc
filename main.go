package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
	"github.com/tarm/serial"
)

const heartbeatMask = 0x3FFF

func main() {
	sensorDevice := flag.String("dev", "", "Serial port device for sensor communication")
	sensorBaud := flag.Int("baud", 57600, "Serial port baud for sensor communication")
	influxAddress := flag.String("influxAddr", "http://localhost:8086", "Address of InfluxDB server")
	logRawCommunication := flag.Bool("logRawCommunication", false, "Log the raw communication with the device")
	flag.Parse()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	influxClient, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr: *influxAddress,
	})
	if err != nil {
		log.Fatalf("influx: %v", err)
	}

	var s io.ReadWriteCloser

	if *sensorDevice != "" && *sensorBaud > 0 {
		c := &serial.Config{Name: *sensorDevice, Baud: *sensorBaud, ReadTimeout: 2 * time.Second}
		s, err = serial.OpenPort(c)
		if err != nil {
			log.Fatal("open port: ", err)
		}
	} else {
		log.Printf("-dev and -baud flags not set, using fakeSerial")
		s = &fakeSerial{}
	}
	defer func() {
		s.Close()
	}()

	var port io.ReadWriter
	port = s
	if *logRawCommunication {
		port = &loggingReadWriter{s}
	}

	// Enable heart beat mode: Geiger counter will report event count every second
	fmt.Fprintf(port, "<HEARTBEAT1>>")
	defer func() {
		fmt.Fprintf(port, "<HEARTBEAT0>>")
	}()

	counts := make(chan uint16, 128)
	go func() {
		defer close(counts)
		for {
			var buf [2]byte
			n, err := port.Read(buf[:])
			if err == io.EOF {
				log.Printf("Read: EOF")
				return
			}
			if err != nil {
				fmt.Printf("Read error: %v\n", err)
				continue
			}
			// After ReadTimeout Read returns with n == 0
			if n == 0 {
				continue
			}

			val := binary.BigEndian.Uint16(buf[:])
			val &= heartbeatMask
			counts <- val
		}
	}()

	cpm := 0
	timer := time.Tick(60 * time.Second)
	for {
		select {
		case sig := <-sigChan:
			log.Printf("Received %s, shutting down", sig)
			return
		case count := <-counts:
			cpm += int(count)
		case <-timer:
			doseRate := float64(cpm) * 0.00625
			log.Printf("cpm=%d, doseRate=%f", cpm, doseRate)
			err = sendToInflux(influxClient, cpm, doseRate)
			if err != nil {
				log.Printf("sendToInflux: %v", err)
			}
			cpm = 0
		}
	}
}

func sendToInflux(influxClient influxdb.Client, cpm int, doseRate float64) error {
	// Create a new point batch
	bp, err := influxdb.NewBatchPoints(influxdb.BatchPointsConfig{
		Database:  "sensors",
		Precision: "s",
	})
	if err != nil {
		return err
	}
	tags := map[string]string{"location": "Office"}
	fields := map[string]interface{}{}
	fields["geiger_counter_cpm"] = cpm
	fields["geiger_counter_dose_rate"] = doseRate

	pt, err := influxdb.NewPoint("measurements", tags, fields, time.Now())
	if err != nil {
		return err
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := influxClient.Write(bp); err != nil {
		return err
	}
	return nil
}

type loggingReadWriter struct {
	rw io.ReadWriter
}

func (l *loggingReadWriter) Read(p []byte) (n int, err error) {
	n, err = l.rw.Read(p)
	log.Printf("loggingReadWriter: Read %d bytes: %x", n, p[0:n])
	return
}

func (l *loggingReadWriter) Write(p []byte) (n int, err error) {
	n, err = l.rw.Write(p)
	log.Printf("loggingReadWriter: Wrote %d bytes: %x", n, p[0:n])
	return
}

type fakeSerial struct {
}

func (l *fakeSerial) Read(p []byte) (n int, err error) {
	p[0] = 0x80
	p[1] = 0x00
	return 2, nil
}

func (l *fakeSerial) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (l *fakeSerial) Close() error {
	return nil
}
