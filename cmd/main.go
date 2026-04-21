package main

import (
	"context"
	"fmt"
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
	)

	root := &cobra.Command{
		Use:   "dbshuffle",
		Short: "Manage pre-created database copies for fast assignment",
	}

	root.PersistentFlags().StringVar(&dbHost, "db-host", envOr("DB_HOST", "localhost"), "MySQL host")
	root.PersistentFlags().IntVar(&dbPort, "db-port", 3306, "MySQL port")
	root.PersistentFlags().StringVar(&dbUser, "db-user", envOr("DB_USER", "root"), "MySQL user")
	root.PersistentFlags().StringVar(&dbPassword, "db-password", envOr("DB_PASSWORD", ""), "MySQL password")
	root.PersistentFlags().StringVar(&cfgPath, "config", "config.yaml", "path to config file")

	setup := func() (*service.ShuffleService, func(), error) {
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return nil, nil, err
		}
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
				for _, db := range r.Assigned {
					fmt.Printf("    [assigned] %s  expires: %s\n", *db.DBName, db.ExpiresAt(r.ExpireHours).Format(time.RFC3339))
				}
				for _, db := range r.Expired {
					fmt.Printf("    [expired]  %s  expired: %s\n", *db.DBName, db.ExpiresAt(r.ExpireHours).Format(time.RFC3339))
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
			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			rec, err := svc.Assign(context.Background(), args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Printf("assigned: %s (expires: %s)\n", rec.DBName, rec.ExpiresAt)
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
			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			rec, err := svc.Extend(context.Background(), args[0], args[1])
			if err != nil {
				return err
			}
			fmt.Printf("extended: %s  new expiry: %s\n", *rec.DBName, rec.ExpiresAt(svc.ExpireHours(args[0])).Format(time.RFC3339))
			return nil
		},
	}
}

func cleanCmd(setup func() (*service.ShuffleService, func(), error)) *cobra.Command {
	return &cobra.Command{
		Use:   "clean",
		Short: "Drop expired databases and remove their records",
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			n, err := svc.Clean(context.Background())
			if err != nil {
				return err
			}
			fmt.Printf("cleaned %d database(s)\n", n)
			return nil
		},
	}
}

func refillCmd(setup func() (*service.ShuffleService, func(), error)) *cobra.Command {
	return &cobra.Command{
		Use:   "refill",
		Short: "Create buffer database copies up to configured levels",
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, done, err := setup()
			if err != nil {
				return err
			}
			defer done()

			n, err := svc.Refill(context.Background())
			if err != nil {
				return err
			}
			fmt.Printf("created %d database(s)\n", n)
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

			// Background refill loop
			go func() {
				ticker := time.NewTicker(refillPeriod)
				defer ticker.Stop()
				for range ticker.C {
					n, err := svc.Refill(context.Background())
					if err != nil {
						fmt.Fprintf(os.Stderr, "refill error: %v\n", err)
						continue
					}
					if n > 0 {
						fmt.Printf("refill: created %d database(s)\n", n)
					}
				}
			}()

			r := chi.NewRouter()
			r.Use(middleware.Logger)
			r.Use(middleware.Recoverer)
			handler.NewShuffleHandler(svc).Routes(r)

			fmt.Printf("listening on %s\n", addr)
			return http.ListenAndServe(addr, r)
		},
	}

	cmd.Flags().StringVar(&addr, "addr", envOr("ADDR", ":8080"), "listen address")
	cmd.Flags().DurationVar(&refillPeriod, "refill-period", time.Minute, "how often to check and refill buffers")
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
