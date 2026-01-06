package rest

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"distributed-task-queue/services/job"

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
	e.POST("/jobs/process", handler.ProcessJob)
	e.POST("/jobs/:id/complete", handler.CompleteJob)
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

	created, err := a.Service.PublishJob(&job)
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
	jobs, err := a.Service.PullJobs(domain.JobStatusPending, 10)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, jobs)
}

// ProcessJobRequest represents the request body for processing a job
type ProcessJobRequest struct {
	QueueID int64 `json:"queue_id" example:"1"`
}

// ProcessJob will process a job from the queue
// @Summary      Process a job from queue
// @Description  Process and lock a job from the specified queue
// @Tags         jobs
// @Accept       json
// @Produce      json
// @Param        request  body      ProcessJobRequest  true  "Process Job Request"
// @Success      200      {object}  domain.Job
// @Failure      400      {string}  string  "Bad Request"
// @Failure      404      {string}  string  "Not Found"
// @Failure      500      {string}  string  "Internal Server Error"
// @Router       /jobs/process [post]
func (a *JobHandler) ProcessJob(c echo.Context) error {
	var req ProcessJobRequest
	err := c.Bind(&req)
	if err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}

	if req.QueueID == 0 {
		return c.JSON(http.StatusBadRequest, "queue_id is required")
	}

	job, err := a.Service.PullJob(req.QueueID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}
	if job == nil {
		return c.JSON(http.StatusOK, nil)
	}
	return c.JSON(http.StatusOK, job)
}

// CompleteJobRequest represents the request body for completing a job
type CompleteJobRequest struct {
	LockToken int64 `json:"lease_token" example:"1"`
}

// CompleteJob will complete a job
// @Summary      Complete a job
// @Description  Mark a job as completed using its ID, lock token
// @Tags         jobs
// @Accept       json
// @Produce      json
// @Param        id       path      int                 true  "Job ID"
// @Param        request  body      CompleteJobRequest  true  "Complete Job Request"
// @Success      200      {object}  domain.Job
// @Failure      400      {string}  string  "Bad Request"
// @Failure      404      {string}  string  "Not Found"
// @Failure      500      {string}  string  "Internal Server Error"
// @Router       /jobs/{id}/complete [post]
func (a *JobHandler) CompleteJob(c echo.Context) error {
	idParam := c.Param("id")
	id, err := strconv.ParseInt(idParam, 10, 64)
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid job id")
	}

	var req CompleteJobRequest
	err = c.Bind(&req)
	if err != nil {
		return c.JSON(http.StatusBadRequest, err.Error())
	}
	reqBody, _ := json.Marshal(req)
	c.Logger().Debug("CompleteJob request body: ", string(reqBody))
	if req.LockToken == 0 {
		return c.JSON(http.StatusBadRequest, "lease_token is required")
	}

	job, err := a.Service.CompleteJob(id, req.LockToken)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, job)
}
