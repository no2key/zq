package tests

import (
	"github.com/mccanne/zq/pkg/test"
	"github.com/mccanne/zq/tests/suite/count"
	"github.com/mccanne/zq/tests/suite/cut"
	"github.com/mccanne/zq/tests/suite/diropt"
	"github.com/mccanne/zq/tests/suite/format"
	"github.com/mccanne/zq/tests/suite/input"
)

var RootDir = "./test-root"

var internals = []test.Internal{
	count.Internal,
	cut.Internal,
	format.Internal,
	input.Internal,
}

var commands = []test.Exec{
	cut.Exec,
}

var scripts = []test.Shell{
	diropt.Test,
	diropt.Test2,
}