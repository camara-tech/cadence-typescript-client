import { afterAll, describe, expect } from 'vitest';
import test from 'vitest-gwt';
import {
  startTestDriver,
  stopTestDriver,
  uniqueDomain,
  uniqueWorkflowId,
  type CadenceTestDriver,
} from './harness/driver.js';

interface Ctx {
  driver: CadenceTestDriver;
  domain: string;
  workflowId: string;
  runId: string;
  result: { status: string; result?: unknown; failure?: { type: string; message: string; reason?: string } };
}

afterAll(async () => {
  await stopTestDriver();
});

describe('Cancellation & Termination context', () => {
  test<Ctx>('cancelling a running workflow closes it as CANCELED', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-cancel');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_long_sleep_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('cancel');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'sleep',
          taskList: 'gwt-tl',
          input: [10_000],
          executionStartToCloseTimeoutMs: 30_000,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
      },
    },
    when: {
      cancelling_the_workflow: async function (this: Ctx) {
        await this.driver.cancelWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
        });
      },
    },
    then: {
      the_run_closes_as_CANCELED: async function (this: Ctx) {
        const result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: 15_000,
        });
        expect(result.status).toBe('CANCELED');
      },
    },
  });

  test<Ctx>('terminating a running workflow closes it as TERMINATED', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-terminate');
        await this.driver.ensureDomain(this.domain);
      },
      a_running_long_sleep_workflow: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('terminate');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'sleep',
          taskList: 'gwt-tl',
          input: [10_000],
          executionStartToCloseTimeoutMs: 30_000,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
      },
    },
    when: {
      terminating_the_workflow_with_a_reason: async function (this: Ctx) {
        await this.driver.terminateWorkflow({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          reason: 'gwt-test-termination',
        });
      },
    },
    then: {
      the_run_closes_as_TERMINATED: async function (this: Ctx) {
        const result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: 15_000,
        });
        expect(result.status).toBe('TERMINATED');
      },
    },
  });
});
