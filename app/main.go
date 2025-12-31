package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	elog "github.com/labstack/gommon/log"
	_ "github.com/lib/pq"
	echoSwagger "github.com/swaggo/echo-swagger"

	"distributed-task-queue/internal/migration"
	postgresRepo "distributed-task-queue/internal/repository/postgresql"
	"distributed-task-queue/internal/rest"
	"distributed-task-queue/services/job"
	"distributed-task-queue/services/queue"

	"github.com/joho/godotenv"
)

const (
	defaultTimeout = 30
	defaultAddress = ":9090"
)

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func main() {
	//prepare database
	dbHost := os.Getenv("DATABASE_HOST")
	dbPort := os.Getenv("DATABASE_PORT")
	dbUser := os.Getenv("DATABASE_USER")
	dbPass := os.Getenv("DATABASE_PASS")
	dbName := os.Getenv("DATABASE_NAME")
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPass, dbName)
	dbConn, err := sql.Open(`postgres`, dsn)
	if err != nil {
		log.Fatal("failed to open connection to database", err)
	}
	err = dbConn.Ping()
	if err != nil {
		log.Fatal("failed to ping database ", err)
	}

	// Run database migrations
	migrationsPath := os.Getenv("MIGRATIONS_PATH")
	if migrationsPath == "" {
		migrationsPath = "migrations"
	}
	if err := migration.RunMigrations(dbConn, migrationsPath); err != nil {
		log.Fatal("failed to run migrations: ", err)
	}

	defer func() {
		err := dbConn.Close()
		if err != nil {
			log.Fatal("got error when closing the DB connection", err)
		}
	}()
	// prepare echo

	e := echo.New()
	e.Logger.SetLevel(elog.DEBUG)
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Swagger endpoint
	e.GET("/swagger/*", echoSwagger.WrapHandler)

	// Prepare Repository
	queueRepo := *postgresRepo.NewQueueRepository(dbConn, e.Logger)
	jobRepo := *postgresRepo.NewJobRepository(dbConn)

	// Build service Layer
	queueSvc := queue.NewQueueService(queueRepo)
	jobSvc := job.NewJobService(jobRepo, queueRepo)
	rest.NewQueueHandler(e, *queueSvc)
	rest.NewJobHandler(e, *jobSvc)

	// Start Server
	address := os.Getenv("SERVER_ADDRESS")
	if address == "" {
		address = defaultAddress
	}
	log.Fatal(e.Start(address)) //nolint
}
