package rest

import (
	"net/http"

	"github.com/labstack/echo/v4"

	job "distributed-task-queue/services"

	"distributed-task-queue/domain"
)

// ResponseError represent the response error struct

// ArticleHandler  represent the httphandler for article
type JobHandler struct {
	Service *job.JobService
}

// NewJobHandler will initialize the jobs/ resources endpoint
func NewJobHandler(e *echo.Echo, svc job.JobService) {
	handler := &JobHandler{
		Service: &svc,
	}
	e.POST("/jobs", handler.PublishJob)
	e.GET("/jobs", handler.PullJobs)
}

// PublishJob will publish the job based on given params
func (a *JobHandler) PublishJob(c echo.Context) error {

	var job domain.Job
	err := c.Bind(&job)
	if err != nil {
		return c.JSON(http.StatusUnprocessableEntity, err.Error())
	}

	ctx := c.Request().Context()

	created, err := a.Service.PublishJob(ctx, job)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, created)
}

// GetByID will get article by given id
func (a *JobHandler) PullJobs(c echo.Context) error {
	ctx := c.Request().Context()

	jobs, err := a.Service.PullJobs(ctx, domain.JobStatusPending, 10)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, jobs)
}
