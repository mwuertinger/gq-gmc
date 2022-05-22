package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"time"

	influxdb "github.com/influxdata/influxdb1-client/v2"
	"github.com/tarm/serial"
)

const heartbeatMask = 0x3FFF

func main() {
	sensorDevice := flag.String("dev", "/dev/ttyUSB0", "Serial port device for sensor communication")
	sensorBaud := flag.Int("baud", 57600, "Serial port baud for sensor communication")
	influxAddress := flag.String("influxAddr", "http://localhost:8086", "Address of InfluxDB server")
	flag.Parse()

	influxClient, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr: *influxAddress,
	})
	if err != nil {
		log.Fatalf("influx: %v", err)
	}

	c := &serial.Config{Name: *sensorDevice, Baud: *sensorBaud}
	s, err := serial.OpenPort(c)
	if err != nil {
		log.Fatal("open port: ", err)
	}
	defer s.Close()

	// Enable heart beat mode: Geiger counter will report event count every second
	fmt.Fprintf(s, "<HEARTBEAT1>>")

	counts := make(chan uint16, 128)
	go func() {
		for {
			var buf [2]byte
			_, err := s.Read(buf[:])
			if err != nil {
				fmt.Printf("Read: %v\n", err)
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
