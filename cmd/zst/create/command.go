package create

import (
	"errors"
	"flag"

	"github.com/brimsec/zq/cmd/zst/root"
	"github.com/brimsec/zq/zbuf"
	"github.com/brimsec/zq/zio"
	"github.com/brimsec/zq/zio/detector"
	"github.com/brimsec/zq/zng/resolver"
	"github.com/brimsec/zq/zst"
	"github.com/mccanne/charm"
)

var Create = &charm.Spec{
	Name:  "create",
	Usage: "create [-coltresh thresh] [-skewthresh thesh] -o file file",
	Short: "create a zst columnar object from a zng file or stream",
	Long: `
The create command generates
xxx.`,
	New: newCommand,
}

func init() {
	root.Zst.Add(Create)
}

type Command struct {
	*root.Command
	colThresh   int
	skewThresh  int
	outputFile  string
	ReaderFlags zio.ReaderFlags
}

func newCommand(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{
		Command: parent.(*root.Command),
	}
	//XXX need byte-friend numbers for thresholds
	f.IntVar(&c.colThresh, "coltresh", 5*1024*1024, "minimum frame size used for zst columns")
	f.IntVar(&c.skewThresh, "skewtresh", 25*1024*1024, "minimum skew size used to group zst columns")
	f.StringVar(&c.outputFile, "o", "", "name of zst output file")
	c.ReaderFlags.SetFlags(f)

	return c, nil
}

func (c *Command) Run(args []string) error {
	//XXX no reason to limit this... we will fix this when we refactor
	// the code here to use zql/proc instead for the hash table (after we
	// have spillable group-bys)
	if len(args) != 1 {
		return errors.New("must specify a single zng input file containing")
	}
	if c.outputFile == "" {
		return errors.New("must specify an output file with -o")
	}
	path := args[0]
	if path == "-" {
		path = detector.StdinPath
	}
	zctx := resolver.NewContext()
	cfg := detector.OpenConfig{
		Format: c.ReaderFlags.Format,
		//JSONTypeConfig: c.jsonTypeConfig,
		//JSONPathRegex:  c.jsonPathRegexp,
	}
	reader, err := detector.OpenFile(zctx, path, cfg)
	if err != nil {
		return err
	}
	defer reader.Close()
	writer, err := zst.NewWriter(zctx, c.outputFile, c.skewThresh, c.colThresh)
	if err != nil {
		return err
	}
	if err := zbuf.Copy(writer, reader); err != nil {
		writer.Abort()
		return err
	}
	return writer.Close()
}
