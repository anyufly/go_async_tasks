package tasks

type SuccessCallback struct {
	OnSucess string
}

func (s SuccessCallback) apply(t *Task) {
	t.Options.OnSucess = s.OnSucess
}

type FailureCallback struct {
	OnFailure string
}

func (f FailureCallback) apply(t *Task) {
	t.Options.OnFailure = f.OnFailure
}
