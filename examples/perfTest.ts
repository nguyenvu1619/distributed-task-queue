import { spawn } from 'child_process';
import * as path from 'path';

type TestType = 'startup' | 'load' | 'latency' | 'all';

interface TestConfig {
  name: string;
  description: string;
  scriptPath: string;
  env?: Record<string, string>;
}

class PerfTestRunner {
  private testConfigs: Record<Exclude<TestType, 'all'>, TestConfig> = {
    startup: {
      name: 'Startup/Shutdown Test',
      description: 'Measures worker initialization and shutdown times',
      scriptPath: path.join(__dirname, 'startup-shutdown-test.ts'),
    },
    load: {
      name: 'Load Test',
      description: 'Measures throughput and performance under load',
      scriptPath: path.join(__dirname, 'performance-test.ts'),
      env: {
        NUM_JOBS: process.env.NUM_JOBS || '10000',
        NUM_WORKERS: process.env.NUM_WORKERS || '100',
        JOB_PROCESSING_TIME_MS: process.env.JOB_PROCESSING_TIME_MS || '0',
      },
    },
    latency: {
      name: 'Latency Test',
      description: 'Measures job execution latency',
      scriptPath: path.join(__dirname, 'latency-test.ts'),
      env: {
        NUM_JOBS: process.env.NUM_JOBS || '100',
        NUM_WORKERS: process.env.NUM_WORKERS || '2',
      },
    },
  };

  async run(testType: TestType) {
    console.log('╔════════════════════════════════════════════════════════════════════════════╗');
    console.log('║              DISTRIBUTED TASK QUEUE PERFORMANCE TEST SUITE                 ║');
    console.log('╚════════════════════════════════════════════════════════════════════════════╝');
    console.log('');

    if (testType === 'all') {
      await this.runAllTests();
    } else {
      await this.runSingleTest(testType);
    }
  }

  private async runAllTests() {
    console.log('Running all performance tests...');
    console.log('');
    console.log('═'.repeat(80));
    console.log('');

    const tests: Array<Exclude<TestType, 'all'>> = ['startup', 'latency', 'load'];
    const results: Array<{ name: string; success: boolean; duration: number }> = [];

    for (let i = 0; i < tests.length; i++) {
      const testType = tests[i];
      const config = this.testConfigs[testType];

      console.log(`\n${'═'.repeat(80)}`);
      console.log(`TEST ${i + 1}/${tests.length}: ${config.name.toUpperCase()}`);
      console.log(`${config.description}`);
      console.log('═'.repeat(80));
      console.log('');

      const startTime = Date.now();
      const success = await this.runSingleTest(testType, false);
      const duration = Date.now() - startTime;

      results.push({
        name: config.name,
        success,
        duration,
      });

      // Wait between tests
      if (i < tests.length - 1) {
        console.log('');
        console.log('⏳ Waiting 2 seconds before next test...');
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    }

    // Print summary
    this.printSummary(results);
  }

  private async runSingleTest(
    testType: Exclude<TestType, 'all'>,
    showHeader: boolean = true
  ): Promise<boolean> {
    const config = this.testConfigs[testType];

    if (showHeader) {
      console.log(`Running: ${config.name}`);
      console.log(`Description: ${config.description}`);
      console.log('');
      console.log('═'.repeat(80));
      console.log('');
    }

    return new Promise((resolve) => {
      const env = {
        ...process.env,
        ...(config.env || {}),
      };

      const child = spawn('npx', ['ts-node', config.scriptPath], {
        env,
        stdio: 'inherit',
      });

      child.on('close', (code) => {
        resolve(code === 0);
      });

      child.on('error', (error) => {
        console.error(`Failed to run ${config.name}:`, error);
        resolve(false);
      });
    });
  }

  private printSummary(results: Array<{ name: string; success: boolean; duration: number }>) {
    console.log('');
    console.log('');
    console.log('╔════════════════════════════════════════════════════════════════════════════╗');
    console.log('║                           TEST SUITE SUMMARY                               ║');
    console.log('╚════════════════════════════════════════════════════════════════════════════╝');
    console.log('');

    const totalDuration = results.reduce((sum, r) => sum + r.duration, 0);
    const successCount = results.filter((r) => r.success).length;
    const failureCount = results.length - successCount;

    console.log('Test Results:');
    results.forEach((result, index) => {
      const status = result.success ? '✅ PASSED' : '❌ FAILED';
      const duration = (result.duration / 1000).toFixed(2);
      console.log(`  ${index + 1}. ${result.name.padEnd(30)} ${status}  (${duration}s)`);
    });
    console.log('');

    console.log('Summary:');
    console.log(`  Total Tests:         ${results.length}`);
    console.log(`  Passed:              ${successCount} ✅`);
    console.log(`  Failed:              ${failureCount} ${failureCount > 0 ? '❌' : ''}`);
    console.log(`  Total Duration:      ${(totalDuration / 1000).toFixed(2)}s`);
    console.log('');

    if (successCount === results.length) {
      console.log('🎉 All tests passed!');
    } else {
      console.log('⚠️  Some tests failed. Please review the output above.');
    }
    console.log('');
    console.log('═'.repeat(80));
  }

  private printUsage() {
    console.log('Usage: npm run perfTest [test-type]');
    console.log('');
    console.log('Test Types:');
    console.log('  startup  - Run startup/shutdown test only');
    console.log('  load     - Run load test only');
    console.log('  latency  - Run latency test only');
    console.log('  all      - Run all tests (default)');
    console.log('');
    console.log('Examples:');
    console.log('  npm run perfTest');
    console.log('  npm run perfTest startup');
    console.log('  npm run perfTest load');
    console.log('  npm run perfTest latency');
    console.log('');
    console.log('Environment Variables (for load test):');
    console.log('  NUM_JOBS              - Number of jobs to process (default: 10000)');
    console.log('  NUM_WORKERS           - Number of workers (default: 100)');
    console.log('  JOB_PROCESSING_TIME_MS - Job processing time in ms (default: 0)');
    console.log('');
    console.log('Environment Variables (for latency test):');
    console.log('  NUM_JOBS              - Number of jobs to measure (default: 100)');
    console.log('  NUM_WORKERS           - Number of workers (default: 2)');
    console.log('');
  }
}

// Main execution
async function main() {
  const args = process.argv.slice(2);
  const testType = (args[0] || 'all') as TestType;

  const validTestTypes: TestType[] = ['startup', 'load', 'latency', 'all'];

  if (args.includes('--help') || args.includes('-h')) {
    const runner = new PerfTestRunner();
    runner['printUsage']();
    process.exit(0);
  }

  if (!validTestTypes.includes(testType)) {
    console.error(`Invalid test type: ${testType}`);
    console.error(`Valid types: ${validTestTypes.join(', ')}`);
    process.exit(1);
  }

  const runner = new PerfTestRunner();
  await runner.run(testType);
  process.exit(0);
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
