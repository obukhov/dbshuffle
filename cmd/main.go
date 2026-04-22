package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/obukhov/dbshuffle/internal/config"
	"github.com/obukhov/dbshuffle/internal/db"
	"github.com/obukhov/dbshuffle/internal/handler"
	"github.com/obukhov/dbshuffle/internal/service"
	"github.com/spf13/cobra"
)

func main() {
	if err := buildRoot().Execute(); err != nil {
		os.Exit(1)
	}
}

func buildRoot() *cobra.Command {
	var (
		dbHost, dbUser, dbPassword string
		dbPort                     int
		cfgPath                    string
		verbose                    bool
	)

	root := &cobra.Command{
		Use:   "dbshuffle",
		Short: "Manage pre-created database copies for fast assignment",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			level := slog.LevelInfo
			if verbose {
				level = slog.LevelDebug
			}
			slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
		},
	}

	root.PersistentFlags().StringVar(&dbHost, "db-host", envOr("DB_HOST", "localhost"), "MySQL host")
	root.PersistentFlags().IntVar(&dbPort, "db-port", 3306, "MySQL port")
	root.PersistentFlags().StringVar(&dbUser, "db-user", envOr("DB_USER", "root"), "MySQL user")
	root.PersistentFlags().StringVar(&dbPassword, "db-password", envOr("DB_PASSWORD", ""), "MySQL password")
	root.PersistentFlags().StringVar(&cfgPath, "config", "config.yaml", "path to config file")
	root.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "enable debug logging")

	setup := func() (*service.ShuffleService, func(), error) {
		slog.Debug("command: loading config", "path", cfgPath)
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return nil, nil, err
		}
		slog.Debug("command: connecting to database", "host", dbHost, "port", dbPort, "user", dbUser)
		database, err := db.Connect(db.Config{
			Host: dbHost, Port: dbPort,
			User: dbUser, Password: dbPassword,
		})
		if err != nil {
			return nil, nil, err
		}
		svc := service.NewShuffleService(database, cfg)
		return svc, func() { database.Close() }, nil
	}

	root.AddCommand(
		statusCmd(setup),
		assignCmd(setup),
		extendCmd(setup),
		cleanCmd(setup),
		refillCmd(setup),
		serverCmd(setup),
	)
	return root
}

func statusCmd(setup func() (*service.ShuffleService, func(), error)) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show status of all managed databases",
		RunE: func(cmd *cobra.Command, args []string) error {
			slog.Info("command: fetching status")
			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			reports, err := svc.Status(context.Background())
			if err != nil {
				return err
			}

			for _, r := range reports {
				fmt.Printf("=== template: %s ===\n", r.Template)
				fmt.Printf("  buffer   : %d\n", len(r.Buffer))
				fmt.Printf("  assigned : %d\n", len(r.Assigned))
				fmt.Printf("  expired  : %d\n", len(r.Expired))
				for _, d := range r.Buffer {
					fmt.Printf("    [buffer]   %s  created: %s\n", d.PhysicalName(), d.CreatedAt.Format(time.RFC3339))
				}
				for _, d := range r.Assigned {
					fmt.Printf("    [assigned] %s  created: %s  expires: %s\n", *d.DBName, d.CreatedAt.Format(time.RFC3339), d.ExpiresAt(r.ExpireHours).Format(time.RFC3339))
				}
				for _, d := range r.Expired {
					fmt.Printf("    [expired]  %s  created: %s  expired: %s\n", *d.DBName, d.CreatedAt.Format(time.RFC3339), d.ExpiresAt(r.ExpireHours).Format(time.RFC3339))
				}
			}
			return nil
		},
	}
}

func assignCmd(setup func() (*service.ShuffleService, func(), error)) *cobra.Command {
	return &cobra.Command{
		Use:   "assign <template> <dbname>",
		Short: "Assign a buffered database copy to the given name",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			template, dbName := args[0], args[1]
			slog.Info("command: assigning database", "template", template, "db_name", dbName)

			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			rec, err := svc.Assign(context.Background(), template, dbName)
			if err != nil {
				return err
			}

			expiresAt := rec.ExpiresAt(svc.ExpireHours(template))
			slog.Info("command: assigned successfully", "db_name", *rec.DBName, "expires_at", expiresAt)
			return nil
		},
	}
}

func extendCmd(setup func() (*service.ShuffleService, func(), error)) *cobra.Command {
	return &cobra.Command{
		Use:   "extend <template> <dbname>",
		Short: "Reset the expiry timer of an assigned database",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			template, dbName := args[0], args[1]
			slog.Info("command: extending database", "template", template, "db_name", dbName)

			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			rec, err := svc.Extend(context.Background(), template, dbName)
			if err != nil {
				return err
			}

			expiresAt := rec.ExpiresAt(svc.ExpireHours(template))
			slog.Info("command: extended successfully", "db_name", *rec.DBName, "new_expires_at", expiresAt)
			return nil
		},
	}
}

func cleanCmd(setup func() (*service.ShuffleService, func(), error)) *cobra.Command {
	return &cobra.Command{
		Use:   "clean",
		Short: "Drop expired databases and remove their records",
		RunE: func(cmd *cobra.Command, args []string) error {
			slog.Info("command: cleaning expired databases")

			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			n, err := svc.Clean(context.Background())
			if err != nil {
				return err
			}

			slog.Info("command: clean complete", "dropped", n)
			return nil
		},
	}
}

func refillCmd(setup func() (*service.ShuffleService, func(), error)) *cobra.Command {
	return &cobra.Command{
		Use:   "refill",
		Short: "Create buffer database copies up to configured levels",
		RunE: func(cmd *cobra.Command, args []string) error {
			slog.Info("command: refilling buffers")

			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			n, err := svc.Refill(context.Background())
			if err != nil {
				return err
			}

			slog.Info("command: refill complete", "created", n)
			return nil
		},
	}
}

func serverCmd(setup func() (*service.ShuffleService, func(), error)) *cobra.Command {
	var (
		addr         string
		refillPeriod time.Duration
	)

	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start the HTTP server with background buffer refill",
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			go func() {
				slog.Info("command: background refill started", "period", refillPeriod)
				ticker := time.NewTicker(refillPeriod)
				defer ticker.Stop()
				for range ticker.C {
					slog.Debug("command: running scheduled refill")
					n, err := svc.Refill(context.Background())
					if err != nil {
						slog.Error("command: scheduled refill failed", "err", err)
						continue
					}
					if n > 0 {
						slog.Info("command: scheduled refill complete", "created", n)
					}
				}
			}()

			r := chi.NewRouter()
			r.Use(middleware.RequestLogger(&slogFormatter{}))
			r.Use(middleware.Recoverer)
			handler.NewShuffleHandler(svc).Routes(r)

			slog.Info("command: server listening", "addr", addr)
			return http.ListenAndServe(addr, r)
		},
	}

	cmd.Flags().StringVar(&addr, "addr", envOr("ADDR", ":8080"), "listen address")
	cmd.Flags().DurationVar(&refillPeriod, "refill-period", time.Minute, "how often to check and refill buffers")
	return cmd
}

// slogFormatter implements chi's middleware.LogFormatter using slog.
type slogFormatter struct{}

func (s *slogFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	return &slogEntry{
		method:     r.Method,
		path:       r.URL.Path,
		remoteAddr: r.RemoteAddr,
	}
}

type slogEntry struct {
	method     string
	path       string
	remoteAddr string
}

func (e *slogEntry) Write(status, bytes int, _ http.Header, elapsed time.Duration, _ interface{}) {
	slog.Info("request",
		"method", e.method,
		"path", e.path,
		"status", status,
		"bytes", bytes,
		"duration", elapsed,
		"remote", e.remoteAddr,
	)
}

func (e *slogEntry) Panic(v interface{}, stack []byte) {
	slog.Error("request panic",
		"method", e.method,
		"path", e.path,
		"panic", v,
		"stack", string(stack),
	)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
