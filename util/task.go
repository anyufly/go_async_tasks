package util

import (
	"reflect"

	"github.com/fakerjeff/go_async_tasks/util/json"

	"github.com/fakerjeff/go_async_tasks/iface/task"
)

func ReflectTaskResults(taskResults []*task.Result) ([]reflect.Value, error) {
	resultValues := make([]reflect.Value, len(taskResults))
	for i, taskResult := range taskResults {
		resultValue, err := ReflectValue(taskResult.Type, taskResult.Value)
		if err != nil {
			return nil, err
		}
		resultValues[i] = resultValue
	}
	return resultValues, nil
}

func UnMarshalTaskResult(data []byte, obj *[]*task.Result) ([]*task.Result, error) {
	err := json.Unmarshal(data, obj)
	if err != nil {
		return nil, err
	}
	return *obj, nil
}
