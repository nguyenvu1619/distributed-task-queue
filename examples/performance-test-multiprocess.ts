import { spawn } from 'child_process';
import * as path from 'path';
import * as os from 'os';

interface ProcessResult {
  processId: number;
  completed: number;
  failed: number;
  duration: number;
}

class MultiProcessRunner {
  private numProcesses: number;
  private pgMaxConnections: number;
  private results: ProcessResult[] = [];
  private startTime: number = 0;

  constructor(
    private numJobs: number,
    private numWorkers: number,
    private workerConcurrency: number,
    private queueConcurrency: number,
    private jobProcessingTimeMs: number
  ) {
    this.numProcesses = parseInt(process.env.NUM_PROCESSES || '4', 10);
    this.pgMaxConnections = parseInt(process.env.PG_MAX_CONNECTIONS || '200', 10);
  }

  async run() {
    // Each process gets an equal slice of the Postgres connection budget.
    const perProcessBudget = Math.floor(this.pgMaxConnections / this.numProcesses);
    // Jobs are divided among processes; each process runs the full worker setup.
    const jobsPerProcess = Math.floor(this.numJobs / this.numProcesses);

    console.log('🚀 Starting Multi-Process Performance Test');
    console.log('Configuration:', {
      numJobs: this.numJobs,
      numWorkers: this.numWorkers,
      workerConcurrency: this.workerConcurrency,
      queueConcurrency: this.queueConcurrency,
      jobProcessingTimeMs: this.jobProcessingTimeMs,
      numProcesses: this.numProcesses,
      pgMaxConnections: this.pgMaxConnections,
      perProcessBudget,
      jobsPerProcess,
      cpuCores: os.cpus().length,
    });
    console.log('─'.repeat(80));

    console.log(`👷 Spawning ${this.numProcesses} child processes...`);
    console.log(`   Each process: ${this.numWorkers} workers × ${this.workerConcurrency} concurrency, ~${jobsPerProcess} jobs, ${perProcessBudget} max connections`);
    console.log('─'.repeat(80));

    this.startTime = Date.now();

    // Spawn all processes
    const processes = [];
    for (let i = 0; i < this.numProcesses; i++) {
      processes.push(this.spawnProcess(i, jobsPerProcess, perProcessBudget));
    }

    // Wait for all processes to complete
    await Promise.all(processes);

    // Print aggregated results
    this.printResults();
  }

  private async spawnProcess(
    processId: number,
    jobsPerProcess: number,
    perProcessBudget: number
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const scriptPath = path.join(__dirname, 'performance-test.ts');
      
      const child = spawn('ts-node', [scriptPath], {
        env: {
          ...process.env,
          PROCESS_ID: processId.toString(),
          NUM_JOBS: jobsPerProcess.toString(),
          NUM_WORKERS: this.numWorkers.toString(),
          WORKER_CONCURRENCY: this.workerConcurrency.toString(),
          QUEUE_CONCURRENCY: this.queueConcurrency.toString(),
          JOB_PROCESSING_TIME_MS: this.jobProcessingTimeMs.toString(),
          PG_MAX_CONNECTIONS: perProcessBudget.toString(),
        },
        stdio: ['inherit', 'pipe', 'pipe'],
      });

      let stdout = '';
      let stderr = '';

      child.stdout.on('data', (data) => {
        const text = data.toString();
        stdout += text;
        // Only show output from first process to avoid clutter
        if (processId === 0) {
          process.stdout.write(text);
        }
      });

      child.stderr.on('data', (data) => {
        const text = data.toString();
        stderr += text;
        if (processId === 0) {
          process.stderr.write(text);
        }
      });

      child.on('close', (code) => {
        if (code === 0) {
          // Try to extract metrics from output
          const completedMatch = stdout.match(/Completed Jobs:\s+(\d+)/);
          const failedMatch = stdout.match(/Failed Jobs:\s+(\d+)/);
          const durationMatch = stdout.match(/Total Duration:\s+(\d+)ms/);

          this.results.push({
            processId,
            completed: completedMatch ? parseInt(completedMatch[1], 10) : 0,
            failed: failedMatch ? parseInt(failedMatch[1], 10) : 0,
            duration: durationMatch ? parseInt(durationMatch[1], 10) : 0,
          });

          resolve();
        } else {
          console.error(`Process ${processId} exited with code ${code}`);
          if (stderr) {
            console.error(`Process ${processId} stderr:`, stderr);
          }
          reject(new Error(`Process ${processId} failed with code ${code}`));
        }
      });
    });
  }

  private printResults() {
    const totalDuration = Date.now() - this.startTime;
    const totalCompleted = this.results.reduce((sum, r) => sum + r.completed, 0);
    const totalFailed = this.results.reduce((sum, r) => sum + r.failed, 0);
    const jobsPerSecond = totalCompleted / (totalDuration / 1000);

    console.log('');
    console.log('═'.repeat(80));
    console.log('📊 MULTI-PROCESS PERFORMANCE TEST RESULTS');
    console.log('═'.repeat(80));
    console.log('');

    console.log('Configuration:');
    console.log(`  Processes:       ${this.numProcesses}`);
    console.log(`  Workers/Process: ${Math.floor(this.numWorkers / this.numProcesses)}`);
    console.log(`  Total Workers:   ${this.numWorkers}`);
    console.log(`  CPU Cores:       ${os.cpus().length}`);
    console.log('');

    console.log('Job Statistics:');
    console.log(`  Total Jobs:      ${this.numJobs}`);
    console.log(`  Completed Jobs:  ${totalCompleted} ✅`);
    console.log(`  Failed Jobs:     ${totalFailed} ❌`);
    console.log(`  Success Rate:    ${((totalCompleted / this.numJobs) * 100).toFixed(2)}%`);
    console.log('');

    console.log('Time Statistics:');
    console.log(`  Total Duration:  ${totalDuration}ms (${(totalDuration / 1000).toFixed(2)}s)`);
    console.log('');

    console.log('Throughput:');
    console.log(`  Jobs/Second:     ${jobsPerSecond.toFixed(2)}`);
    console.log(`  Jobs/Minute:     ${(jobsPerSecond * 60).toFixed(2)}`);
    console.log('');

    console.log('Process Statistics:');
    this.results.forEach((result) => {
      console.log(`  Process ${result.processId}:     Completed=${result.completed}, Failed=${result.failed}, Duration=${result.duration}ms`);
    });
    console.log('');
    console.log('═'.repeat(80));
  }
}

// Main execution
async function main() {
  const numJobs = parseInt(process.env.NUM_JOBS || '100000', 10);
  const numWorkers = parseInt(process.env.NUM_WORKERS || '4', 10);
  const workerConcurrency = parseInt(process.env.WORKER_CONCURRENCY || '24', 10);
  const queueConcurrency = parseInt(process.env.QUEUE_CONCURRENCY || '0', 10);
  const jobProcessingTimeMs = parseInt(process.env.JOB_PROCESSING_TIME_MS || '0', 10);

  const runner = new MultiProcessRunner(
    numJobs,
    numWorkers,
    workerConcurrency,
    queueConcurrency,
    jobProcessingTimeMs
  );

  await runner.run();
  process.exit(0);
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
