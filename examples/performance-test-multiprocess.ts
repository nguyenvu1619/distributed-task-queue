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
  private results: ProcessResult[] = [];
  private startTime: number = 0;

  constructor(
    private numJobs: number,
    private numWorkers: number,
    private queueConcurrency: number,
    private jobProcessingTimeMs: number
  ) {
    this.numProcesses = parseInt(process.env.NUM_PROCESSES || '4', 10);
  }

  async run() {
    console.log('🚀 Starting Multi-Process Performance Test');
    console.log('Configuration:', {
      numJobs: this.numJobs,
      numWorkers: this.numWorkers,
      queueConcurrency: this.queueConcurrency,
      jobProcessingTimeMs: this.jobProcessingTimeMs,
      numProcesses: this.numProcesses,
      cpuCores: os.cpus().length,
    });
    console.log('─'.repeat(80));

    // Calculate jobs and workers per process
    const jobsPerProcess = Math.floor(this.numJobs / this.numProcesses);
    const workersPerProcess = Math.floor(this.numWorkers / this.numProcesses);

    console.log(`👷 Spawning ${this.numProcesses} child processes...`);
    console.log(`   Each process: ${workersPerProcess} workers, ~${jobsPerProcess} jobs`);
    console.log('─'.repeat(80));

    this.startTime = Date.now();

    // Spawn all processes
    const processes = [];
    for (let i = 0; i < this.numProcesses; i++) {
      processes.push(this.spawnProcess(i, jobsPerProcess, workersPerProcess));
    }

    // Wait for all processes to complete
    await Promise.all(processes);

    // Print aggregated results
    this.printResults();
  }

  private async spawnProcess(
    processId: number,
    jobsPerProcess: number,
    workersPerProcess: number
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const scriptPath = path.join(__dirname, 'performance-test.ts');
      
      const child = spawn('ts-node', [scriptPath], {
        env: {
          ...process.env,
          PROCESS_ID: processId.toString(),
          NUM_JOBS: jobsPerProcess.toString(),
          NUM_WORKERS: workersPerProcess.toString(),
          QUEUE_CONCURRENCY: this.queueConcurrency.toString(),
          JOB_PROCESSING_TIME_MS: this.jobProcessingTimeMs.toString(),
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
  const numWorkers = parseInt(process.env.NUM_WORKERS || '1000', 10);
  const queueConcurrency = parseInt(process.env.QUEUE_CONCURRENCY || '0', 10);
  const jobProcessingTimeMs = parseInt(process.env.JOB_PROCESSING_TIME_MS || '0', 10);

  const runner = new MultiProcessRunner(
    numJobs,
    numWorkers,
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
