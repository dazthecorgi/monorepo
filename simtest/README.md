# Quilibrium Simulation Test Environment

This directory contains to run multiple Quilibrium archive nodes locally with Docker for testing purposes.

## Prequisites

- Go toolchain
- Docker

## Quick Start

```
go run .
```

See `go run . -help` for more config options.

## Development

Run `./ci.sh` to test changes.

## Common Issues

If you get the following error:

```
Error response from daemon: all predefined address pools have been fully subnetted
```

You need to decrease the capacity of each bridge network so that Docker can allocate more networks.
You can achieve this by adding the following to `/etc/docker/daemon.json`:

```json
{
  "default-address-pools" : [
    {
      "base" : "172.17.0.0/12",
      "size" : 20
    },
    {
      "base" : "192.168.0.0/16",
      "size" : 24
    }
  ]
}
```

And then run `sudo systemctl restart docker` to apply the changes.

Read [this](https://straz.to/2021-09-08-docker-address-pools/) article if you're interested in more details.

