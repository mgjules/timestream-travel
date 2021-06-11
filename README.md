# Timestream Travel

A simple CLI application to backup time-series data from Amazon Timestream to S3.

- [Timestream Travel](#timestream-travel)
  - [Download the binary](#download-the-binary)
  - [OR build it yourself](#or-build-it-yourself)
    - [Requirements](#requirements)
  - [Example usage](#example-usage)
  - [Usage documentation](#usage-documentation)

## Download the binary

Get the [latest binary](https://github.com/RingierIMU/timestream-travel/releases/latest) from the releases.

## OR build it yourself

### Requirements
 - Go 1.16+


```shell
$ go mod tidy
$ go build .
```

## Example usage

```shell
$ ./timestream-travel backup --verbose --from "2021-06-09 00:00:00" --to "2021-06-12 00:00:00" --rows 200
```

## Usage documentation

```shell
$ ./timestream-travel -h
$ ./timestream-travel backup -h
```