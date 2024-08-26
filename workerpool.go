package workerpool

import (
	"context"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

const (
	defaultParallel = 10
)

type DBConf struct {
	URL    string
	Schema string
	Upsert string
}

type WorkerFunc func(param interface{}) ([]interface{}, error)

type WorkerPool struct {
	parallel int
	logger   log.FieldLogger
	f        WorkerFunc
	db       *sqlx.DB
	dbconf   DBConf

	afterImportHooks []Hook
}

type Hook func(*sqlx.DB)

type Option func(*WorkerPool) error

func WithParallel(p int) Option {
	return func(wp *WorkerPool) error {
		if p > 1000 {
			return fmt.Errorf("%d is too high. only values <= 1000 are allowed", p)
		}

		wp.parallel = p
		return nil
	}
}

func WithAfterImportHook(f Hook) Option {
	return func(wp *WorkerPool) error {
		wp.afterImportHooks = append(wp.afterImportHooks, f)
		return nil
	}
}

func New(dbconf DBConf, fun WorkerFunc, opts ...Option) (*WorkerPool, error) {
	wp := &WorkerPool{
		parallel: defaultParallel,
		logger:   log.New().WithField("component", "workerpool"),
		f:        fun,
		dbconf:   dbconf,
	}

	for _, o := range opts {
		if err := o(wp); err != nil {
			return nil, err
		}
	}

	return wp, nil
}

func (wp *WorkerPool) Init() error {
	db, err := sqlx.Connect("postgres", wp.dbconf.URL)
	if err != nil {
		return err
	}
	wp.db = db
	if wp.dbconf.Schema != "" {
		wp.logger.Infof("executing migration...")
		_, err = wp.db.Exec(wp.dbconf.Schema)
	}
	return err
}

func (wp *WorkerPool) Shutdown() error {
	wp.db.Close()
	return nil
}

func (wp *WorkerPool) Run(ctx context.Context, paramC <-chan interface{}) {
	var wg sync.WaitGroup

	for i := 0; i < wp.parallel; i++ {
		wg.Add(1)
		wp.logger.Infof("starting worker %d", i)
		go func(n int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case param, ok := <-paramC:
					if param == nil && !ok {
						return
					}
					out, err := wp.f(param)
					if err != nil {
						wp.logger.WithField("worker", n).Errorf("error while executing func: %+v", err)
						continue
					}
					for _, item := range out {
						if _, err := wp.db.NamedExec(wp.dbconf.Upsert, item); err != nil {
							wp.logger.WithField("worker", n).Errorf("error while upserting item: %+v", err)
							continue
						}
					}
				}
			}
		}(i)
	}
	wg.Wait()

	for _, hook := range wp.afterImportHooks {
		hook(wp.db)
	}
}
