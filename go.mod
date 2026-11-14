module github.com/mwuertinger/gq-gmc

// go 1.21.0 is the minimum that allows a toolchain line; language semantics
// stay pre-1.22 (no per-iteration loop variables).
go 1.21.0

toolchain go1.27.4

require (
	github.com/influxdata/influxdb1-client v0.0.0-20220302092344-a9ab5670611c
	github.com/tarm/serial v0.0.0-20180830185346-98f6abe2eb07
)

require golang.org/x/sys v0.0.0-20220412211240-33da011f77ad // indirect

// Keep the go1.20 GODEBUG defaults the binary was built with before the go
// line was raised (e.g. panicnil=1). Settings that Go 1.27 removed
// (asynctimerchan, tls10server, tls3des, tlsrsakex, tlsunsafeekm,
// x509keypairleaf, gotypesalias) are silently dropped from this default.
godebug default=go1.20
