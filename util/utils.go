package util

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"net"
	"os"
	"time"
)

func GetDefaultEnv(key string, defaultValue string) string {
	result := os.Getenv(key)
	if result == "" {
		return defaultValue
	}
	return result
}

func Min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func Pow(x int, n int) int {
	r := math.Pow(float64(x), float64(n))
	return int(math.Floor(r))
}

func Round(val float64, precision int) float64 {
	p := math.Pow10(precision)
	return math.Floor(val*p+0.5) / p
}

func RandFloat64(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}

func ComputeBackoff(retries int, minBackoff time.Duration, maxBackoff time.Duration, random bool) time.Duration {
	min := int(minBackoff)
	max := int(maxBackoff)

	backoff := Min(Pow(2, retries)*min, max)
	if random {
		halfBackoff := float64(backoff) / 2
		rand.Seed(HashSeed())
		ramdom := RandFloat64(0, 1)
		backoff = int(Round(halfBackoff+(ramdom*halfBackoff), 0))
	}
	return time.Duration(backoff)
}

func GetMacAddrs() (string, error) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, netInterface := range netInterfaces {
		macAddr := netInterface.HardwareAddr.String()
		if len(macAddr) == 0 {
			continue
		}
		return macAddr, nil
	}
	return "", err
}

func Hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func HashSeed() int64 {
	macAddr, _ := GetMacAddrs()
	return int64(Hash(fmt.Sprintf("%s-%d", macAddr, time.Now().UnixNano())))
}
