package mysqlstore

import (
	"database/sql"
	"log"
	"time"

	"github.com/admpub/sessions"
	sqlstore "github.com/coscms/session-sqlstore"
	"github.com/webx-top/echo/encoding/dbconfig"
	ss "github.com/webx-top/echo/middleware/session/engine"
	"github.com/webx-top/echo/middleware/session/engine/file"
)

var DefaultMaxReconnect = 5

func New(cfg *Options) sessions.Store {
	cfg.Config.Engine = `mysql`
	eng, err := NewMySQLStore(cfg)
	if err != nil {
		retries := cfg.MaxReconnect
		if retries <= 0 {
			retries = DefaultMaxReconnect
		}
		for i := 1; i < retries; i++ {
			log.Println(`[sessions]`, err.Error())
			wait := time.Second
			log.Printf(`[sessions] (%d/%d) reconnect mysql after %v`, i, retries, wait)
			time.Sleep(wait)
			eng, err = NewMySQLStore(cfg)
			if err == nil {
				log.Println(`[sessions] reconnect mysql successfully`)
				return eng
			}
		}
	}
	if err != nil {
		log.Println("sessions: Operation MySQL failed:", err)
		return file.NewFilesystemStore(&file.FileOptions{
			SavePath:      ``,
			KeyPairs:      cfg.KeyPairs,
			CheckInterval: cfg.CheckInterval,
		})
	}
	return eng
}

func Reg(store sessions.Store, args ...string) {
	name := `mysql`
	if len(args) > 0 {
		name = args[0]
	}
	ss.Reg(name, store)
}

func RegWithOptions(opts *Options, args ...string) sessions.Store {
	store := New(opts)
	Reg(store, args...)
	return store
}

type Options struct {
	Config dbconfig.Config `json:"-"`
	sqlstore.Options
}

type MySQLStore struct {
	*sqlstore.SQLStore
}

const DDL = "CREATE TABLE IF NOT EXISTS %s (" +
	"	`id` char(64) NOT NULL," +
	"	`data` longblob NOT NULL," +
	"	`created` int(11) unsigned NOT NULL DEFAULT '0'," +
	"	`modified` int(11) unsigned NOT NULL DEFAULT '0'," +
	"	`expires` int(11) unsigned NOT NULL DEFAULT '0'," +
	"	PRIMARY KEY (`id`)" +
	"  ) ENGINE=InnoDB;"

// NewMySQLStore takes the following paramaters
// endpoint - A sql.Open style endpoint
// tableName - table where sessions are to be saved. Required fields are created automatically if the table doesnot exist.
// path - path for Set-Cookie header
// maxAge
// codecs
func NewMySQLStore(cfg *Options) (*MySQLStore, error) {
	db, err := sql.Open("mysql", cfg.Config.String())
	if err != nil {
		return nil, err
	}

	return NewMySQLStoreFromConnection(db, cfg)
}

// NewMySQLStoreFromConnection .
func NewMySQLStoreFromConnection(db *sql.DB, cfg *Options) (*MySQLStore, error) {
	cfg.Options.SetDDL(DDL)
	base, err := sqlstore.New(db, &cfg.Options)
	if err != nil {
		return nil, err
	}
	s := &MySQLStore{
		SQLStore: base,
	}
	return s, nil
}
