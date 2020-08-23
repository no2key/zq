package inspect

import (
	"errors"
	"flag"
	"os"

	"github.com/brimsec/zq/cmd/zst/root"
	"github.com/brimsec/zq/emitter"
	"github.com/brimsec/zq/zbuf"
	"github.com/brimsec/zq/zio"
	"github.com/brimsec/zq/zng/resolver"
	"github.com/brimsec/zq/zst"
	"github.com/mccanne/charm"
	"golang.org/x/crypto/ssh/terminal"
)

var Inspect = &charm.Spec{
	Name:  "inspect",
	Usage: "inspect [flags] path",
	Short: "look at info in a zst file",
	Long: `
The inspect command extracts information from a zst file.
This is mostly useful for test and debugging, though there may be interetsting
uses as the zst format becomes richer with pruning information and other internal
aggregations about the columns and so forth.
The -R option sends the reassembly zng data to the output while
the -trailer option indicates that the trailer should be included.

See the zst command help for a description of a zst object.`,
	New: newCommand,
}

func init() {
	root.Zst.Add(Inspect)
}

type Command struct {
	*root.Command
	outputFile   string
	WriterFlags  zio.WriterFlags
	textShortcut bool
	forceBinary  bool
	trailer      bool
	reassembly   bool
}

func newCommand(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.BoolVar(&c.textShortcut, "t", false, "use format tzng independent of -f option")
	f.BoolVar(&c.forceBinary, "B", false, "allow binary zng be sent to a terminal output")
	f.BoolVar(&c.trailer, "trailer", false, "include the zst trailer in the output")
	f.BoolVar(&c.reassembly, "R", false, "include the zst reassembly section in the output")
	c.WriterFlags.SetFlags(f)
	return c, nil
}

func isTerminal(f *os.File) bool {
	return terminal.IsTerminal(int(f.Fd()))
}

func (c *Command) Run(args []string) error {
	//XXX TBD: if no args specified dump various stats about the zst file
	if len(args) != 1 {
		return errors.New("zst inspect: must be run with a single path argument")
	}
	if c.textShortcut {
		c.WriterFlags.Format = "tzng"
	}
	if c.WriterFlags.Format == "zng" && isTerminal(os.Stdout) && !c.forceBinary {
		return errors.New("writing binary zng data to terminal; override with -B or use -t for text.")
	}
	path := args[0]
	reader, err := zst.NewReader(resolver.NewContext(), path)
	if err != nil {
		return err
	}
	defer reader.Close()
	writer, err := emitter.NewFile(c.outputFile, &c.WriterFlags)
	if err != nil {
		return err
	}
	defer func() {
		if writer != nil {
			writer.Close()
		}
	}()
	if c.reassembly {
		r := reader.NewReassemblyReader()
		if err := zbuf.Copy(writer, r); err != nil {
			return err
		}
	}
	if c.trailer {
		r := reader.NewTrailerReader()
		if err := zbuf.Copy(writer, r); err != nil {
			return err
		}
	}
	err = writer.Close()
	writer = nil
	return err
}
