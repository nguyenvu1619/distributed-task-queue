package rest

import (
	"net/http"

	"github.com/labstack/echo/v4"

	job "distributed-task-queue/services"

	"distributed-task-queue/domain"
)

// JobHandler  represent the httphandler for job
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
// @Summary      Publish a new job
// @Description  Create and publish a new job to the queue
// @Tags         jobs
// @Accept       json
// @Produce      json
// @Param        job  body      domain.Job  true  "Job object"
// @Success      200  {object}  domain.Job
// @Failure      422  {string}  string  "Unprocessable Entity"
// @Failure      500  {string}  string  "Internal Server Error"
// @Router       /jobs [post]
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

// PullJobs will pull pending jobs from the queue
// @Summary      Pull pending jobs
// @Description  Retrieve pending jobs from the queue
// @Tags         jobs
// @Accept       json
// @Produce      json
// @Success      200  {array}   domain.Job
// @Failure      500  {string}  string  "Internal Server Error"
// @Router       /jobs [get]
func (a *JobHandler) PullJobs(c echo.Context) error {
	ctx := c.Request().Context()

	jobs, err := a.Service.PullJobs(ctx, domain.JobStatusPending, 10)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, jobs)
}
