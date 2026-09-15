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

const TIMEOUT_MS = 60_000;

afterAll(async () => {
  await stopTestDriver();
});

describe('Timeout context', () => {
  test<Ctx>('an activity exceeding its timeout fails the workflow with a timeout', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-timeout');
        await this.driver.ensureDomain(this.domain);
      },
      a_workflow_running_an_activity_longer_than_its_timeout: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('activity-timeout');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'activity-timeout',
          workflowId: this.workflowId,
          taskList: 'gwt-tl',
          input: [],
          executionStartToCloseTimeoutMs: TIMEOUT_MS,
        });
        this.runId = started.runId;
      },
    },
    when: {
      waiting_for_the_run_to_close: async function (this: Ctx) {
        this.result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: TIMEOUT_MS,
        });
      },
    },
    then: {
      the_workflow_fails: function (this: Ctx) {
        expect(this.result.status).toBe('FAILED');
      },
      the_failure_type_is_Timeout: function (this: Ctx) {
        expect(this.result.failure?.type).toBe('Timeout');
      },
    },
  });

  test<Ctx>('a workflow exceeding its execution timeout times out', {
    given: {
      a_running_driver: async function (this: Ctx) {
        this.driver = await startTestDriver();
      },
      a_registered_domain: async function (this: Ctx) {
        this.domain = uniqueDomain('gwt-exec-timeout');
        await this.driver.ensureDomain(this.domain);
      },
      a_sleeping_workflow_with_a_short_execution_timeout: async function (this: Ctx) {
        this.workflowId = uniqueWorkflowId('exec-timeout');
        const started = await this.driver.startWorkflow({
          domain: this.domain,
          workflowType: 'sleep',
          taskList: 'gwt-tl',
          input: [30_000],
          executionStartToCloseTimeoutMs: 3_000,
          workflowId: this.workflowId,
        });
        this.runId = started.runId;
      },
    },
    when: {
      waiting_for_the_run_to_close: async function (this: Ctx) {
        this.result = await this.driver.waitForClose({
          domain: this.domain,
          workflowId: this.workflowId,
          runId: this.runId,
          timeoutMs: 30_000,
        });
      },
    },
    then: {
      the_run_closes_as_TIMED_OUT: function (this: Ctx) {
        expect(this.result.status).toBe('TIMED_OUT');
      },
    },
  });
});
