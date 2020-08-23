package root

import (
	"flag"

	"github.com/mccanne/charm"
)

var Zst = &charm.Spec{
	Name:  "zst",
	Usage: "zst <command> [options] [arguments...]",
	Short: "create and manipulate zst columnar objects",
	Long: `
zst is command-line utility for creating and manipulating zst columnar objects.`,
	New: func(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
		return &Command{}, nil
	},
}

func init() {
	Zst.Add(charm.Help)
}

type Command struct{}

func (c *Command) Run(args []string) error {
	if len(args) == 0 {
		return Zst.Exec(c, []string{"help"})
	}
	return charm.ErrNoRun
}
