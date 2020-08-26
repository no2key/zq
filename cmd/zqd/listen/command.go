package listen

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
	"os/signal"

	"github.com/brimsec/zq/cmd/zqd/logger"
	"github.com/brimsec/zq/cmd/zqd/root"
	"github.com/brimsec/zq/pkg/fs"
	"github.com/brimsec/zq/pkg/httpd"
	"github.com/brimsec/zq/proc"
	"github.com/brimsec/zq/zqd"
	"github.com/brimsec/zq/zqd/zeek"
	"github.com/mccanne/charm"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

var Listen = &charm.Spec{
	Name:  "listen",
	Usage: "listen [options]",
	Short: "listen as a daemon and repond to zqd service requests",
	Long: `
The listen command launches a process to listen on the provided interface and
`,
	New: New,
}

func init() {
	root.Zqd.Add(Listen)
}

type Command struct {
	*root.Command
	listenAddr string
	conf       zqd.Config
	pprof      bool
	prom       bool
	// brimfd is a file descriptor passed through by brim desktop, that zqd uses
	// to determine if brim is still alive. If set zqd will exit if the fd is
	// closed.
	brimfd         int
	zeekRunnerPath string
	configfile     string
	loggerConf     *logger.Config
	logLevel       zapcore.Level
	logger         *zap.Logger
	devMode        bool
	portFile       string
}

func New(parent charm.Command, f *flag.FlagSet) (charm.Command, error) {
	c := &Command{Command: parent.(*root.Command)}
	f.StringVar(&c.listenAddr, "l", ":9867", "[addr]:port to listen on")
	f.StringVar(&c.conf.Root, "data", ".", "data location")
	f.StringVar(&c.zeekRunnerPath, "zeekrunner", "", "path to command that generates zeek logs from pcap data")
	f.BoolVar(&c.pprof, "pprof", false, "add pprof routes to api")
	f.BoolVar(&c.prom, "prometheus", false, "add prometheus metrics routes to api")
	f.StringVar(&c.configfile, "config", "", "path to a zqd config file")
	f.Var(&c.logLevel, "loglevel", "level for log output (defaults to info)")
	f.BoolVar(&c.devMode, "dev", false, "runs zqd in development mode")
	f.StringVar(&c.portFile, "portfile", "", "write port of http listener to file")

	// hidden
	f.IntVar(&c.brimfd, "brimfd", -2, "pipe")
	return c, nil
}

func (c *Command) Run(args []string) error {
	if err := c.init(); err != nil {
		return err
	}
	core, err := zqd.NewCore(c.conf)
	if err != nil {
		return err
	}
	c.logger.Info("Starting",
		zap.String("datadir", c.conf.Root),
		zap.Bool("pprof_routes", c.pprof),
		zap.Bool("zeek_supported", core.HasZeek()),
	)
	h := zqd.NewHandler(core, c.logger)
	if c.pprof {
		h = pprofHandlers(h)
	}
	if c.prom {
		h = prometheusHandlers(h)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if c.brimfd != -2 {
		if ctx, err = c.watchBrimFd(ctx); err != nil {
			return err
		}
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		sig := <-ch
		c.logger.Info("Signal received", zap.Stringer("signal", sig))
		cancel()
	}()
	srv := httpd.New(c.listenAddr, h)
	srv.SetLogger(c.logger.Named("httpd"))
	if err := srv.Start(ctx); err != nil {
		return err
	}
	if c.portFile != "" {
		if err := c.writePortFile(srv.Addr()); err != nil {
			return err
		}
	}
	return srv.Wait()
}

func (c *Command) init() error {
	if err := c.loadConfigFile(); err != nil {
		return err
	}
	if err := c.initLogger(); err != nil {
		return err
	}
	return c.initZeek()
}

func (c *Command) watchBrimFd(p context.Context) (context.Context, error) {
	f := os.NewFile(uintptr(c.brimfd), "brimfd")
	c.logger.Info("Listening to brim process pipe", zap.String("fd", f.Name()))
	ctx, cancel := context.WithCancel(p)
	b := make([]byte, 10)
	go func() {
		var err error
		for {
			_, err = f.Read(b)
			if err != nil {
				break
			}
		}
		fmt.Println("got err", err)
		cancel()
	}()
	return ctx, nil
}

func pprofHandlers(h http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", h)
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// XXX Eventually this function should take prometheus.Registry as an argument.
// For now since we only care about retrieving go stats, create registry
// here.
func prometheusHandlers(h http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", h)
	promreg := prometheus.NewRegistry()
	promreg.MustRegister(prometheus.NewGoCollector())
	promhandler := promhttp.HandlerFor(promreg, promhttp.HandlerOpts{})
	mux.Handle("/metrics", promhandler)
	return mux
}

// Example configfile
// logger:
//   type: waterfall
//   children:
//   - path: ./data/access.log
//     name: "http.access"
//     level: info
//     mode: truncate
// sort_mem_max_bytes: 268432640

func (c *Command) loadConfigFile() error {
	if c.configfile == "" {
		return nil
	}
	conf := &struct {
		Logger          logger.Config `yaml:"logger"`
		SortMemMaxBytes *int          `yaml:"sort_mem_max_bytes,omitempty"`
	}{}
	b, err := ioutil.ReadFile(c.configfile)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(b, conf)
	c.loggerConf = &conf.Logger
	if v := conf.SortMemMaxBytes; v != nil {
		if *v <= 0 {
			return fmt.Errorf("%s: sortMemMaxBytes value must be greater than zero", c.configfile)
		}
		proc.SortMemMaxBytes = *v
	}
	return err
}

func (c *Command) initZeek() error {
	if c.zeekRunnerPath == "" {
		var err error
		if c.zeekRunnerPath, err = exec.LookPath("zeekrunner"); err != nil {
			return nil
		}
	}
	ln, err := zeek.LauncherFromPath(c.zeekRunnerPath)
	if err != nil {
		return err
	}
	c.conf.ZeekLauncher = ln
	return nil
}

// defaultLogger ignores output from the access logger.
func (c *Command) defaultLogger() *logger.Config {
	return &logger.Config{
		Type: logger.TypeWaterfall,
		Children: []logger.Config{
			{
				Name:  "http.access",
				Path:  "/dev/null",
				Level: c.logLevel,
			},
			{
				Path:  "stderr",
				Level: c.logLevel,
			},
		},
	}
}

func (c *Command) initLogger() error {
	if c.loggerConf == nil {
		c.loggerConf = c.defaultLogger()
	}
	core, err := logger.NewCore(*c.loggerConf)
	if err != nil {
		return err
	}
	// If the development mode is on, calls to logger.DPanic will cause a panic
	// whereas in production would result in an error.
	var opts []zap.Option
	if c.devMode {
		opts = append(opts, zap.Development())
	}
	c.logger = zap.New(core, opts...)
	c.conf.Logger = c.logger
	return nil
}

func (c *Command) writePortFile(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	return fs.ReplaceFile(c.portFile, 0644, func(w io.Writer) error {
		_, err := w.Write([]byte(port))
		return err
	})
}
