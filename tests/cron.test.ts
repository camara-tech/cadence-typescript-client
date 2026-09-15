import { afterAll, describe, expect } from 'vitest';
import test from 'vitest-gwt';
import {
  startTestDriver,
  stopTestDriver,
  uniqueDomain,
  uniqueWorkflowId,
  type CadenceTestDriver,
} from './harness/driver.js';
import { pollUntil } from './harness/helpers.js';

interface Ctx {
  driver: CadenceTestDriver;
  domain: string;
  workflowId: string;
  runId: string;
  closeStatus: string;
}

afterAll(async () => {
  await stopTestDriver();
});

describe('Cron context', () => {
  test<Ctx>('a cron workflow continues as new after its first run completes', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-cron');
        await this.driver.ensureDomain(this.domain);
      },
      an_echo_workflow_started_with_a_minutely_cron: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('cron-echo');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'echo',
          taskList: 'gwt-tl',
          input: ['cron-run'],
          executionStartToCloseTimeoutMs: 90_000,
          cronSchedule: '* * * * *',
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
      },
    },
    when: {
      waiting_for_the_first_run_to_close: async function (this: Ctx) {
        // The first run closes as ContinuedAsNew and the cron starts the next
        // run; poll describe until the first run reports a close status.
        await pollUntil(async () => {
          const desc = await this.driver.describeWorkflowExecution({
            domain: this.domain,
            workflowId: this.workflowId,
            runId: this.runId,
          });
          this.closeStatus = desc.status;
          return desc.status !== 'RUNNING';
        }, { timeoutMs: 120_000, intervalMs: 2_000 });
      },
    },
    then: {
      the_first_run_closes_as_CONTINUED_AS_NEW: function (this: Ctx) {
        expect(this.closeStatus).toBe('CONTINUED_AS_NEW');
      },
    },
  });
});
