package util

import "strings"

func XQName(queueName string) string {
	if strings.HasSuffix(queueName, ".XQ") {
		return queueName
	}

	if strings.HasSuffix(queueName, ".DQ") {
		queueName = queueName[0 : len(queueName)-3]
	}
	return queueName + ".XQ"
}

func DQName(queueName string) string {
	if strings.HasSuffix(queueName, ".DQ") {
		return queueName
	}

	if strings.HasSuffix(queueName, ".XQ") {
		queueName = queueName[0 : len(queueName)-3]
	}
	return queueName + ".DQ"
}

func QName(queueName string) string {
	if strings.HasSuffix(queueName, ".XQ") || strings.HasSuffix(queueName, ".DQ") {
		return queueName[0 : len(queueName)-3]
	}
	return queueName
}
