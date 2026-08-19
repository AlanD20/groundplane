package runner

import "context"

// FakeRunner is the L1 hermetic-test double: no real subprocess ever
// runs. Register a Result (or an error) per command Name, or set RunFunc
// for full control; every call is recorded in Calls for assertions. See
// docs/standards.md, section 13 (L1 — hermetic tests).
type FakeRunner struct {
	Calls   []RunCmdOpts
	Results map[string]Result
	Errors  map[string]error
	RunFunc func(ctx context.Context, opts RunCmdOpts) (Result, error)
}

func NewFake() *FakeRunner {
	return &FakeRunner{Results: map[string]Result{}, Errors: map[string]error{}}
}

func (f *FakeRunner) Run(ctx context.Context, opts RunCmdOpts) (Result, error) {
	f.Calls = append(f.Calls, opts)
	if f.RunFunc != nil {
		return f.RunFunc(ctx, opts)
	}
	if err, ok := f.Errors[opts.Name]; ok {
		return Result{}, err
	}
	return f.Results[opts.Name], nil
}

func (f *FakeRunner) Stream(ctx context.Context, opts RunCmdOpts, onLine func(stderr bool, line string)) (Result, error) {
	res, err := f.Run(ctx, opts)
	for _, line := range splitLines(res.Stdout) {
		onLine(false, line)
	}
	return res, err
}
